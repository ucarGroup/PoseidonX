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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.api.opereators.BaseDataSourceOperator;
import com.huawei.streaming.api.opereators.DataSourceOperator;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.builder.operatorconverter.DataSourceConverter;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateDataSourceAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.DatasourceBodyDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.JoinExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.StreamAliasDesc;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 数据源拆分算子
 *
 */
public class DataSourceSplitter extends SelectSplitter
{
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceSplitter.class);
    
    private DataSourceConverter dataSourceConverter = new DataSourceConverter();
    
    /**
     * <默认构造函数>
     *
     */
    public DataSourceSplitter(BuilderUtils buildUtils)
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
        
        return clauseContext.getJoinexpression().getRightExpression() instanceof DatasourceBodyDesc;
    }
    
    /**
     * Join算子拆分
     * Join时候，先查询该流所有列的Join结果，之后再进行列过滤。
     * 目前暂时不支持多表Join
     * <p/>
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
            LOG.error("Too many stream in datasource splitter from clause.", exception);
            throw exception;
        }
        
        checkNaturalJoin(joinExpressionDesc);
        
        StreamAliasDesc leftDesc = (StreamAliasDesc)joinExpressionDesc.getLeftExpression();
        DatasourceBodyDesc rightDesc = (DatasourceBodyDesc)joinExpressionDesc.getRightExpression();
        
        DataSourceOperator dataSourceOp = createDataSource(leftDesc, rightDesc);
        if (dataSourceConverter.validate(dataSourceOp))
        {
            getResult().addOperators(dataSourceConverter.convert(dataSourceOp));
        }
        else
        {
            getResult().addOperators(dataSourceOp);
        }
    }
    
    private DataSourceOperator createDataSource(StreamAliasDesc leftDesc, DatasourceBodyDesc rightDesc)
        throws ApplicationBuildException
    {
        String opName = getBuildUtils().getNextOperatorName("DataSource");
        int parNumber = getParallelNumber();
        
        DataSourceOperator dataSourceOp = new DataSourceOperator(opName, parNumber);
        dataSourceOp.setLeftStreamName(leftDesc.getStreamName());
        setFilterAfterJoinExpression(dataSourceOp);
        dataSourceOp.setOutputExpression(getSelectClauseContext().toString());
        setFilterBeforeWindow(getFromClauseContext(), leftDesc, dataSourceOp);
        
        BuilderUtils builderUtils = DriverContext.getBuilderNameSpace().get();
        String dataSourceName = rightDesc.getDatasourceName();
        CreateDataSourceAnalyzeContext dataSourceDefine = builderUtils.getDataSourceDefineByName(dataSourceName);
        if (dataSourceDefine == null)
        {
            ApplicationBuildException exception = new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_DATASOURCE_UNKNOWN, dataSourceName);
            LOG.error("Unknown dataSource : {}", dataSourceName, exception);
            throw exception;
        }
        
        dataSourceOp.setDataSourceClassName(dataSourceDefine.getDataSourceClass());
        dataSourceOp.setDataSourceConfig(dataSourceDefine.getDataSourceConfig());
        dataSourceOp.setQueryArguments(rightDesc.getQueryArguments());
        dataSourceOp.setDataSourceSchema(rightDesc.getSchema());
        dataSourceOp.setFilterAfterAggregate(parseHaving());
        dataSourceOp.setGroupbyExpression(parseGroupby());
        dataSourceOp.setOrderBy(parseOrderBy());
        dataSourceOp.setLimit(parseLimit());
        return dataSourceOp;
    }
    
    private void setFilterAfterJoinExpression(BaseDataSourceOperator datasourceOp)
    {
        if (getWhereClauseContext() != null)
        {
            datasourceOp.setFilterAfterJoinExpression(getWhereClauseContext().toString());
        }
    }
    
    /**
     * 检查是否是naturalJoin，如果是，就抛出异常
     * <p/>
     * 暂时不支持Natural Join，意义不大，估计很久也不会支持。
     * 两表做自然连接时，两表中的所有名称相同的列都将被比较，这是隐式的。
     * 自然连接得到的结果表中，两表中名称相同的列只出现一次.
     * 在 Oracle 里用 JOIN USING 或 NATURAL JOIN 时，
     * 如果两表共有的列的名称前加上某表名作为前缀，
     * 则会报编译错误: "ORA-25154: column part of USING clause cannot have qualifier"
     * 或 "ORA-25155: column used in NATURAL join cannot have qualifier".
     *
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
        BaseDataSourceOperator dataSourceOp)
        throws ApplicationBuildException
    {
        for (Entry<String, Window> et : clauseContext.getWindows().entrySet())
        {
            String streamName = et.getKey();
            Window win = et.getValue();
            String leftAlias =
                Strings.isNullOrEmpty(leftDesc.getStreamAlias()) ? leftDesc.getStreamName() : leftDesc.getStreamAlias();
            if (streamName.equals(leftAlias))
            {
                FilterOperator leftFilterOp = splitFiterBeforeWindow(streamName);
                OperatorTransition leftTransition = createTransition(leftFilterOp, dataSourceOp, streamName);
                dataSourceOp.setLeftStreamName(leftTransition.getStreamName());
                dataSourceOp.setLeftWindow(win);
                getResult().addOperators(leftFilterOp);
                getResult().addTransitions(leftTransition);
            }
        }
    }
    
    private boolean isNaturalJoin(JoinExpressionDesc joinExpressionDesc)
    {
        return joinExpressionDesc.getJointype().equals(JoinType.NATURAL_JOIN);
    }
}
