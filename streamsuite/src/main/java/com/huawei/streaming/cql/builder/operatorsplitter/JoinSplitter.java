/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.cql.builder.operatorsplitter;

import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.UniDiRectionType;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.DatasourceBodyDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.JoinExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.StreamAliasDesc;
import com.huawei.streaming.exception.ErrorCode;

/**
 * Join语句拆分
 * 
 */
public class JoinSplitter extends SelectSplitter
{
    private static final Logger LOG = LoggerFactory.getLogger(JoinSplitter.class);
    
    /**
     * <默认构造函数>
     */
    public JoinSplitter(BuilderUtils buildUtils)
    {
        super(buildUtils);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(AnalyzeContext parseContext)
    {
        if (!(parseContext instanceof SelectAnalyzeContext))
        {
            return false;
        }
        
        SelectAnalyzeContext selectAnalzyeContext = (SelectAnalyzeContext)parseContext;
        FromClauseAnalyzeContext clauseContext = selectAnalzyeContext.getFromClauseContext();
        if (clauseContext.getCombineConditions().size() != 0)
        {
            return false;
        }
        
        if (clauseContext.getJoinexpression() == null)
        {
            return false;
        }
        
        if (clauseContext.getJoinexpression().getRightExpression() instanceof DatasourceBodyDesc)
        {
            return false;
        }
        
        return true;
    }
    
    /**
     * Join算子拆分
     * Join时候，先查询该流所有列的Join结果，之后再进行列过滤。
     * 目前暂时不支持多表Join
     * 
     * Join中如果包含了group by，仅会影响到Join算子内部数据的Group by
     * 不会导致数据在进入Join算子的时候，数据分发的改变。
     * 数据分发还是按照On的条件来进行分发。
     * {@inheritDoc}
     */
    @Override
    protected void splitFromClause()
        throws ApplicationBuildException
    {
        
        JoinExpressionDesc joinExpressionDesc = getFromClauseContext().getJoinexpression();
        if (!(joinExpressionDesc.getLeftExpression() instanceof StreamAliasDesc)
            && !(joinExpressionDesc.getRightExpression() instanceof StreamAliasDesc))
        {
            ApplicationBuildException exception = new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_MULTI_JOIN);
            LOG.error("Too many stream in join split from clause.", exception);
            throw exception;
        }
        
        checkNaturalJoin(joinExpressionDesc);
        
        StreamAliasDesc leftDesc = (StreamAliasDesc)joinExpressionDesc.getLeftExpression();
        StreamAliasDesc rightDesc = (StreamAliasDesc)joinExpressionDesc.getRightExpression();
        
        ExpressionDescribe joinCondiiton = joinExpressionDesc.getJoinCondition();
        
        String opName = getBuildUtils().getNextOperatorName("Join");
        int parNumber = getParallelNumber();
        JoinFunctionOperator joinop = new JoinFunctionOperator(opName, parNumber);
        joinop.setLeftStreamName(leftDesc.getStreamName());
        joinop.setRightStreamName(rightDesc.getStreamName());
        
        setUnidirection(getFromClauseContext(), leftDesc, joinop);
        resetJoinType(joinExpressionDesc, joinCondiiton, joinop);
        setFilterAfterJoinExpression(joinop);
        
        joinop.setOutputExpression(getSelectClauseContext().toString());
        
        setFilterBeforeWindow(getFromClauseContext(), leftDesc, rightDesc, joinop);
        
        joinop.setFilterAfterAggregate(parseHaving());
        joinop.setGroupbyExpression(parseGroupby());
        joinop.setOrderBy(parseOrderBy());
        joinop.setLimit(parseLimit());
        getResult().addOperators(joinop);
        
    }
    
    private void setFilterAfterJoinExpression(JoinFunctionOperator joinop)
    {
        if (getWhereClauseContext() != null)
        {
            joinop.setFilterAfterJoinExpression(getWhereClauseContext().toString());
        }
    }
    
    /**
     * 重新设置Join类型
     * 
     * 在语法解析器中，系统会将
     * a join b 解析成inner join
     * 但是，这样就无法区分inner join和cross join
     * 所以，这里需要明确，是否是cross join
     * 
     * 在逻辑优化器中，系统会将where中的等值表达式提升到on上面去，作为Join的条件。
     * 如果没有显示指明Join的条件，那么就直接是Cross Join
     * 当然，这个Join必须是inner join，其他的join不可以。
     * 
     */
    private void resetJoinType(JoinExpressionDesc joinExpressionDesc, ExpressionDescribe joinCondiiton,
        JoinFunctionOperator joinop)
        throws ApplicationBuildException
    {
        if (joinCondiiton != null)
        {
            joinop.setJoinExpression(joinCondiiton.toString());
            joinop.setJoinType(joinExpressionDesc.getJointype());
        }
        else
        {
            if (!joinExpressionDesc.getJointype().equals(JoinType.INNER_JOIN)
                && !joinExpressionDesc.getJointype().equals(JoinType.CROSS_JOIN))
            {
                ApplicationBuildException exception =
                    new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_JOIN_NO_CONDITION);
                LOG.error("Don't have join condition.", exception);
                throw exception;
                
            }
            joinop.setJoinType(JoinType.CROSS_JOIN);
        }
    }
    
    private void setUnidirection(FromClauseAnalyzeContext clauseContext, StreamAliasDesc leftDesc,
        JoinFunctionOperator joinop)
    {
        if (clauseContext.getUniDirections() == null)
        {
            joinop.setUniDirectionIndex(UniDiRectionType.NONE_STREAM);
        }
        else if (clauseContext.getUniDirections().equals(leftDesc.getStreamName()))
        {
            joinop.setUniDirectionIndex(UniDiRectionType.LEFT_STREAM);
        }
        else if (!StringUtils.isEmpty(leftDesc.getStreamAlias())
            && clauseContext.getUniDirections().equals(leftDesc.getStreamAlias()))
        {
            joinop.setUniDirectionIndex(UniDiRectionType.LEFT_STREAM);
        }
        else
        {
            joinop.setUniDirectionIndex(UniDiRectionType.RIGHT_STREAM);
        }
    }
    
    /**
     * 检查是否是naturalJoin，如果是，就抛出异常
     * 
     * 暂时不支持Natural Join，意义不大，估计很久也不会支持。
     * 两表做自然连接时，两表中的所有名称相同的列都将被比较，这是隐式的。
     * 自然连接得到的结果表中，两表中名称相同的列只出现一次.
     * 在 Oracle 里用 JOIN USING 或 NATURAL JOIN 时，
     * 如果两表共有的列的名称前加上某表名作为前缀，
     * 则会报编译错误: "ORA-25154: column part of USING clause cannot have qualifier" 
     * 或 "ORA-25155: column used in NATURAL join cannot have qualifier".
     */
    private void checkNaturalJoin(JoinExpressionDesc joinExpressionDesc)
        throws ApplicationBuildException
    {
        if (isNaturalJoin(joinExpressionDesc))
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_JOIN_UNSPPORTTED_NATURAL_JOIN);
            LOG.error("Unsupport natural join.", exception);
            throw exception;
            
        }
    }
    
    private void setFilterBeforeWindow(FromClauseAnalyzeContext clauseContext, StreamAliasDesc leftDesc,
        StreamAliasDesc rightDesc, JoinFunctionOperator joinop)
        throws ApplicationBuildException
    {
        for (Entry<String, Window> et : clauseContext.getWindows().entrySet())
        {
            String streamName = et.getKey();
            Window win = et.getValue();
            String leftAlias =
                Strings.isNullOrEmpty(leftDesc.getStreamAlias()) ? leftDesc.getStreamName() : leftDesc.getStreamAlias();
            String rightAlias =
                Strings.isNullOrEmpty(rightDesc.getStreamAlias()) ? rightDesc.getStreamName()
                    : rightDesc.getStreamAlias();
            if (streamName.equals(leftAlias))
            {
                FilterOperator leftFilterOp = splitFiterBeforeWindow(streamName);
                OperatorTransition leftTransition = createTransition(leftFilterOp, joinop, streamName);
                joinop.setLeftStreamName(leftTransition.getStreamName());
                joinop.setLeftWindow(win);
                getResult().addOperators(leftFilterOp);
                getResult().addTransitions(leftTransition);
            }
            if (streamName.equals(rightAlias))
            {
                FilterOperator rightFilterOp = splitFiterBeforeWindow(streamName);
                OperatorTransition rightTransition = createTransition(rightFilterOp, joinop, streamName);
                joinop.setRightStreamName(rightTransition.getStreamName());
                joinop.setRightWindow(win);
                getResult().addOperators(rightFilterOp);
                getResult().addTransitions(rightTransition);
            }
        }
    }
    
    private boolean isNaturalJoin(JoinExpressionDesc joinExpressionDesc)
    {
        return joinExpressionDesc.getJointype().equals(JoinType.NATURAL_JOIN);
    }
}
