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

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.JoinFunctionOperator;
import com.huawei.streaming.api.opereators.JoinType;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FilterClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.LimitClauseAnalzyeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.OrderByClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.ParallelClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.JoinExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.StreamAliasDesc;
import com.huawei.streaming.cql.semanticanalyzer.expressiondescwalker.ExpressionDescsWalker;
import com.huawei.streaming.cql.semanticanalyzer.expressiondescwalker.IndexWalkerStrategy;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.ExpressionOperator;
import com.huawei.streaming.process.sort.SortEnum;

/**
 * select语句的拆分以及Schema
 * 1、最一般的select子句。
 * 只有一个schema输入和输出都是一个schema
 * 不论有没有窗口，都必须放在聚合算子中。
 * 只要有where，就都放在functor算子中，在优化器中，再进行调整，可以改为filter或者继承再聚合算子中。
 * 2、Join
 * 多个Join的schema，一个outoutschema
 * 先查询出多个表所有的列，再在functor算子中进行列过滤。
 * 3、Groupby
 * 聚合算子，
 * 4、orderby
 * 同一般select子句。
 * 5、Join语句中不支持聚合和groupby，至少目前不支持。
 * 6、三种过滤
 * 窗口之后的过滤：where，放在聚合算子中
 * 窗口之前的过滤：filter，前面加一个filter算子
 * 但是这样就牵扯到schema的变化，这个就麻烦一些了。
 * 先解析出所有的列，再进行过滤。
 * 聚合之后的过滤：having，放在聚合算子中
 * <p/>
 * 总结下：
 * 1、聚合算子是必须有的。
 * 2、Orderby必须放在独立sort算子中
 * 3、limit放在output中作为限制，但是目前还不支持。
 * 4、一个select语句，如论如何拆分，都只有一个输出schema
 * 至少目前是这样，后面在优化器中会进行调整，将where中的一些列加入到select中，进行一些列变换。
 * 5、Join时候，先查询该流所有列的Join结果，之后再进行列过滤。
 * 6、Sort、Join、Aggregate算子都是按照字段进行分发，其他都是随机分发。
 * <p/>
 * TODO orderby limit暂时不支持
 *
 */
public abstract class SelectSplitter implements Splitter
{
    private static final Logger LOG = LoggerFactory.getLogger(SelectSplitter.class);
    
    private BuilderUtils buildUtils;
    
    private SplitContext result = new SplitContext();
    
    private SelectAnalyzeContext selectAnalyzeContext;
    
    private SelectClauseAnalyzeContext selectClauseContext;
    
    private FromClauseAnalyzeContext fromClauseContext;
    
    private FilterClauseAnalzyeContext whereClauseContext;
    
    private SelectClauseAnalyzeContext groupbyClauseContext;

    private OrderByClauseAnalyzeContext orderbyClauseContext;
    
    private FilterClauseAnalzyeContext havingClauseContext;
    
    private LimitClauseAnalzyeContext limitClauseContext;
    
    private ParallelClauseAnalyzeContext parallelClauseContext;
    
    private int parallelNumber = 1;
    
    /**
     * <默认构造函数>
     *
     */
    public SelectSplitter(BuilderUtils buildUtils)
    {
        this.buildUtils = buildUtils;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SplitContext split(AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        initParameters(parseContext);
        
        setParallelNumber();
        splitFromClause();
        
        result.setParseContext(selectAnalyzeContext);
        return result;
    }
    
    public SplitContext getResult()
    {
        return result;
    }
    
    public int getParallelNumber()
    {
        return parallelNumber;
    }
    
    public SelectClauseAnalyzeContext getSelectClauseContext()
    {
        return selectClauseContext;
    }
    
    public FromClauseAnalyzeContext getFromClauseContext()
    {
        return fromClauseContext;
    }
    
    public FilterClauseAnalzyeContext getWhereClauseContext()
    {
        return whereClauseContext;
    }
    
    public SelectClauseAnalyzeContext getGroupbyClauseContext()
    {
        return groupbyClauseContext;
    }

    public OrderByClauseAnalyzeContext getOrderbyClauseContext()
    {
        return orderbyClauseContext;
    }
    
    public FilterClauseAnalzyeContext getHavingClauseContext()
    {
        return havingClauseContext;
    }
    
    public LimitClauseAnalzyeContext getLimitClauseContext()
    {
        return limitClauseContext;
    }
    
    public ParallelClauseAnalyzeContext getParallelClauseContext()
    {
        return parallelClauseContext;
    }
    
    public BuilderUtils getBuildUtils()
    {
        return buildUtils;
    }
    
    /**
     * from子句拆分
     *
     */
    protected abstract void splitFromClause()
        throws ApplicationBuildException;
    
    /**
     * limit子句拆分
     *
     */
    protected Integer parseLimit()
        throws ApplicationBuildException
    {
        if (getLimitClauseContext() != null)
        {
            return Integer.valueOf(getLimitClauseContext().toString());
        }
        return null;
    }
    
    /**
     * sort by子句处理
     *
     */
    protected String parseOrderBy()
        throws ApplicationBuildException
    {
        
        if (getOrderbyClauseContext() == null)
        {
            return null;
        }
        
        List<Pair<String, Integer>> sorderBys = walkOrderBy();
        if (sorderBys == null || sorderBys.size() == 0)
        {
            return null;
        }
        
        OrderByClauseAnalyzeContext sortContext = getOrderbyClauseContext();
        String havingExps = sortContext.toString();
        for (Pair<String, Integer> ps : sorderBys)
        {
            String formattedIndex = SpliterTmps.formatIndex(ps.getSecond());
            String strExp = ps.getFirst();
            havingExps = havingExps.replaceAll(strExp, formattedIndex);
        }
        return havingExps;
        
    }
    
    /**
     * group by 子句处理
     *
     */
    protected String parseGroupby()
    {
        return getGroupbyClauseContext() != null ? getGroupbyClauseContext().toString() : null;
    }
    
    /**
     * Having子句处理
     *
     */
    protected String parseHaving()
        throws ApplicationBuildException
    {
        if (getHavingClauseContext() == null)
        {
            return null;
        }
        List<Pair<String, Integer>> havingPairs = walkHaving();
        if (havingPairs == null || havingPairs.size() == 0)
        {
            return null;
        }
        
        FilterClauseAnalzyeContext havingContext = getHavingClauseContext();
        String havingExps = havingContext.getExpdes().get(0).toString();
        for (Pair<String, Integer> ps : havingPairs)
        {
            String formattedIndex = SpliterTmps.formatIndex(ps.getSecond());
            String strExp = ps.getFirst();
            havingExps = havingExps.replaceAll(strExp, formattedIndex);
        }
        return havingExps;
    }
    
    /**
     * filter before window 语句解析
     *
     */
    protected FilterOperator splitFiterBeforeWindow(String streamName)
        throws SemanticAnalyzerException
    {
        FromClauseAnalyzeContext clauseContext = getFromClauseContext();
        FilterOperator fop = new FilterOperator(buildUtils.getNextOperatorName("Filter"), parallelNumber);
        ExpressionDescribe expression = clauseContext.getFilterBeForeWindow().get(streamName);
        if (expression == null)
        {
            fop.setFilterExpression(null);
        }
        else
        {
            fop.setFilterExpression(expression.toString());
        }
        
        fop.setOutputExpression(createFilterOutputExpression(streamName));
        return fop;
    }
    
    /**
     * 创建连线
     *
     */
    protected OperatorTransition createTransition(Operator fromOp, Operator toOp, String streamName)
        throws ApplicationBuildException
    {
        FromClauseAnalyzeContext clauseContext = getFromClauseContext();
        
        DistributeType distype = DistributeType.SHUFFLE;
        String disFields = null;
        Schema schema = clauseContext.getInputSchemas().get(0);
        
        if (getGroupbyClauseContext() != null)
        {
            disFields = removeDataSourceColumnsFromGroupbyExpression(schema, getGroupbyClauseContext().toString());
            distype = DistributeType.FIELDS;
        }
        
        if (toOp instanceof JoinFunctionOperator)
        {
            List<Schema> inputSchemas = getFromClauseContext().getInputSchemas();
            schema = BaseAnalyzer.getSchemaByName(streamName, inputSchemas);
            
            if (((JoinFunctionOperator)toOp).getJoinType() != JoinType.CROSS_JOIN)
            {
                disFields = getJoinExpression(schema);
                distype = DistributeType.FIELDS;
            }
        }
        
        return new OperatorTransition(buildUtils.getNextStreamName(), fromOp, toOp, distype, disFields, schema);
    }
     
    private void initParameters(AnalyzeContext parseContext)
    {
        selectAnalyzeContext = (SelectAnalyzeContext)parseContext;
        selectClauseContext = selectAnalyzeContext.getSelectClauseContext();
        fromClauseContext = selectAnalyzeContext.getFromClauseContext();
        whereClauseContext = selectAnalyzeContext.getWhereClauseContext();
        groupbyClauseContext = selectAnalyzeContext.getGroupbyClauseContext();
        orderbyClauseContext = selectAnalyzeContext.getOrderbyClauseContext();
        havingClauseContext = selectAnalyzeContext.getHavingClauseContext();
        limitClauseContext = selectAnalyzeContext.getLimitClauseContext();
        parallelClauseContext = selectAnalyzeContext.getParallelClauseContext();
    }
    
    private void setParallelNumber()
    {
        if (getParallelClauseContext() == null || getParallelClauseContext().getParallelNumber() == null)
        {
            parallelNumber = buildUtils.getDefaultParallelNumber();
        }
        else
        {
            parallelNumber = getParallelClauseContext().getParallelNumber();
        }
    }
    
    /**
     * 判断having和sort中的列是否包含原始列或者别名列，
     * 如果包含，则用select列表中的顺序列来替换
     * 如果不包含，则判断表达式是否和select列表中的表达式相同，
     * 如果相同，则使用select列表中的顺序列来替换
     * 如果不相同，则报错，having或者sort错误。
     *
     */
    private List<Pair<String, Integer>> walkOrderBy()
        throws ApplicationBuildException
    {
        
        OrderByClauseAnalyzeContext orderbyContext = getOrderbyClauseContext();
        IndexWalkerStrategy strategy = new IndexWalkerStrategy(selectAnalyzeContext);
        ExpressionDescsWalker getter = new ExpressionDescsWalker(strategy);
        
        if (orderbyContext == null || orderbyContext.getOrderbyExpressions().size() == 0)
        {
            return null;
        }
        
        /*
         * 每次遍历完毕之后，都需要检查表达式是否已经增长
         * 防止有不匹配的发生
         */
        List<ExpressionDescribe> eqExpressions = Lists.newArrayList();
        for (Pair<ExpressionDescribe, SortEnum> ps : orderbyContext.getOrderbyExpressions())
        {
            int startSize = eqExpressions.size();
            getter.found(ps.getFirst(), eqExpressions);
            checkExpressionSize(eqExpressions, ps.getFirst(), startSize);
        }
        
        /*
         * 检查索引大小和表达式的数量是否相等
         */
        checkIndexSize(orderbyContext, strategy, eqExpressions);
        List<Pair<String, Integer>> res = Lists.newArrayList();
        for (int i = 0; i < eqExpressions.size(); i++)
        {
            res.add(new Pair<String, Integer>(eqExpressions.get(i).toString(), strategy.getIndexs().get(i)));
        }
        return res;
        
    }
    
    private void checkIndexSize(AnalyzeContext orderbyContext, IndexWalkerStrategy strategy,
        List<ExpressionDescribe> eqExpressions)
        throws ApplicationBuildException
    {
        if (strategy.getIndexs().size() != eqExpressions.size())
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_NO_EXPRESSION_IN_SELECT, orderbyContext.toString());
            LOG.error("Expression quantity not match.", exception);
            throw exception;
        }
    }
    
    private void checkExpressionSize(List<ExpressionDescribe> eqExpressions, ExpressionDescribe expDesc, int startSize)
        throws ApplicationBuildException
    {
        if (startSize == eqExpressions.size())
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_NO_EXPRESSION_IN_SELECT, expDesc.toString());
            LOG.error("Expression quantity not match.", exception);
            throw exception;
        }
    }
    
    /**
     * 遍历having表达式
     * 判断having和sort中的列是否包含原始列或者别名列，
     * 如果包含，则用select列表中的顺序列来替换
     * 如果不包含，则判断表达式是否和select列表中的表达式相同，
     * 如果相同，则使用select列表中的顺序列来替换
     * 如果不相同，则报错，having或者sort错误。
     *
     */
    private List<Pair<String, Integer>> walkHaving()
        throws ApplicationBuildException
    {
        IndexWalkerStrategy strategy = new IndexWalkerStrategy(selectAnalyzeContext);
        ExpressionDescsWalker getter = new ExpressionDescsWalker(strategy);
        FilterClauseAnalzyeContext havingContext = getHavingClauseContext();
        List<ExpressionDescribe> havings = havingContext.getExpdes();
        
        if (havings == null || havings.size() == 0)
        {
            return null;
        }
        
        /*
         * 每次遍历完毕之后，都需要检查表达式是否已经增长
         * 防止有不匹配的发生
         */
        List<ExpressionDescribe> eqExpressions = Lists.newArrayList();
        for (ExpressionDescribe expression : havings)
        {
            int startSize = eqExpressions.size();
            getter.found(expression, eqExpressions);
            checkExpressionSize(eqExpressions, expression, startSize);
        }
        
        /*
         * 检查索引大小和表达式的数量是否相等
         */
        checkIndexSize(havingContext, strategy, eqExpressions);
        List<Pair<String, Integer>> res = Lists.newArrayList();
        for (int i = 0; i < eqExpressions.size(); i++)
        {
            res.add(new Pair<String, Integer>(eqExpressions.get(i).toString(), strategy.getIndexs().get(i)));
        }
        return res;
    }
    
    private String createFilterOutputExpression(String streamName)
        throws SemanticAnalyzerException
    {
        FromClauseAnalyzeContext clauseContext = getFromClauseContext();
        Schema schema = BaseAnalyzer.getSchemaByName(streamName, clauseContext.getInputSchemas());
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < schema.getCols().size(); i++)
        {
            sb.append(schema.getId() + "." + schema.getCols().get(i).getName());
            if (i != schema.getCols().size() - 1)
            {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    /*
     * 数据源group by的时候，要移除数据源schema中的列
     */
    private String removeDataSourceColumnsFromGroupbyExpression(Schema schema, String groupbyExpression)
        throws ApplicationBuildException
    {
        List<String> groupbyExps = Lists.newArrayList();
        String[] fields = groupbyExpression.trim().split(",");
        for (String field : fields)
        {
            String[] stremAndField = field.trim().split("\\.");
            if (stremAndField.length == 1)
            {
                groupbyExps.add(field);
                continue;
            }
            
            String streamName = stremAndField[0].trim();
            if (!streamName.equals(schema.getStreamName()) && !streamName.equals(schema.getId())
                && !streamName.equals(schema.getName()))
            {
                LOG.info("remove dataSource columns for OperatorTransition");
                continue;
            }
            groupbyExps.add(field);
        }
        return Joiner.on(", ").join(groupbyExps);
    }
    
    private String getJoinExpression(Schema schema)
        throws ApplicationBuildException
    {
        JoinExpressionDesc joinExpressionDesc = getFromClauseContext().getJoinexpression();
        
        validateMultiJoin(joinExpressionDesc);
        
        ExpressionDescribe condition = joinExpressionDesc.getJoinCondition();
        
        if (checkJoinCondition(condition))
        {
            return null;
        }
        
        checkJoinConditionExpressions(condition);
        
        BinaryExpressionDesc bdesc = (BinaryExpressionDesc)condition;
        
        joinCheck(bdesc);
        StringBuilder sb = new StringBuilder();
        getOneSlideJoinExpression(bdesc, sb, schema);
        return sb.toString();
    }
    
    private void checkJoinConditionExpressions(ExpressionDescribe condition)
        throws ApplicationBuildException
    {
        if (!(condition instanceof BinaryExpressionDesc))
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, condition.toString());
            LOG.error("Not BinaryExpression.", exception);
            throw exception;
        }
    }
    
    private boolean checkJoinCondition(ExpressionDescribe condition)
    {
        if (condition == null)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_JOIN_NO_CONDITION);
            LOG.error("Don't have join condition.", exception);
            return true;
        }
        return false;
    }
    
    private void validateMultiJoin(JoinExpressionDesc joinExpressionDesc)
        throws ApplicationBuildException
    {
        if (!(joinExpressionDesc.getLeftExpression() instanceof StreamAliasDesc)
            && !(joinExpressionDesc.getRightExpression() instanceof StreamAliasDesc))
        {
            ApplicationBuildException exception = new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_MULTI_JOIN);
            LOG.error("Unsupport more than two stream join.", exception);
            throw exception;
        }
    }
    
    private void getOneSlideJoinExpression(BinaryExpressionDesc bdesc, StringBuilder sb, Schema schema)
        throws ApplicationBuildException
    {
        
        if (isSimpleJoin(bdesc))
        {
            createSimpleJoinExpression(bdesc, sb, schema);
            return;
        }
        
        getOneSlideJoinExpression((BinaryExpressionDesc)bdesc.getArgExpressions().get(0), sb, schema);
        getOneSlideJoinExpression((BinaryExpressionDesc)bdesc.getArgExpressions().get(1), sb, schema);
    }
    
    private void createSimpleJoinExpression(BinaryExpressionDesc bdesc, StringBuilder sb, Schema schema)
        throws ApplicationBuildException
    {
        PropertyValueExpressionDesc leftDesc = (PropertyValueExpressionDesc)bdesc.getArgExpressions().get(0);
        PropertyValueExpressionDesc rightDesc = (PropertyValueExpressionDesc)bdesc.getArgExpressions().get(1);
        
        if (isInSchema(leftDesc.getSchemaId(), schema))
        {
            if (sb.length() != 0)
            {
                sb.append(",");
            }
            sb.append(leftDesc.getProperty());
            return;
        }
        
        if (isInSchema(rightDesc.getSchemaId(), schema))
        {
            if (sb.length() != 0)
            {
                sb.append(",");
            }
            sb.append(rightDesc.getProperty());
            return;
        }
        
        /*
         * 左流和右流中都找不到对应的列
         */
        ApplicationBuildException exception = new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_JOIN_NO_COLUMN);
        LOG.error("Can't find column in stream from join condition.", exception);
        
        throw exception;
    }
    
    /**
     * Join条件检查
     * 目前只支持等值Join，并且多个join的条件必须是and
     *
     */
    private void joinCheck(BinaryExpressionDesc bdesc)
        throws ApplicationBuildException
    {
        if (isSimpleJoin(bdesc))
        {
            checkSimpleJoin(bdesc);
            return;
        }
        
        checkJoinConditionLogicAnd(bdesc);
        checkJoinConditionExpressions(bdesc.getArgExpressions().get(0));
        checkJoinConditionExpressions(bdesc.getArgExpressions().get(1));
        joinCheck((BinaryExpressionDesc)bdesc.getArgExpressions().get(0));
        joinCheck((BinaryExpressionDesc)bdesc.getArgExpressions().get(1));
    }
    
    private void checkJoinConditionLogicAnd(BinaryExpressionDesc bdesc)
        throws ApplicationBuildException
    {
        if (!(bdesc.getBexpression().getType() == ExpressionOperator.LOGICAND))
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, bdesc.toString());
            LOG.error("Unsupported join condition , support logic and.", exception);
            throw exception;
        }
    }
    
    private void checkSimpleJoin(BinaryExpressionDesc bdesc)
        throws ApplicationBuildException
    {
        checkJoinConditionRelation(bdesc);
        checkJoinConditionPropertyValueExpression(bdesc.getArgExpressions().get(0));
        checkJoinConditionPropertyValueExpression(bdesc.getArgExpressions().get(1));
    }
    
    private void checkJoinConditionPropertyValueExpression(ExpressionDescribe bdesc)
        throws ApplicationBuildException
    {
        if (!(bdesc instanceof PropertyValueExpressionDesc))
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, bdesc.toString());
            LOG.error("Unsupport join condition, condition must be property expression.", exception);
            throw exception;
        }
    }
    
    private void checkJoinConditionRelation(BinaryExpressionDesc bdesc)
        throws ApplicationBuildException
    {
        if (!(bdesc.getBexpression().getType() == ExpressionOperator.EQUAL))
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION, bdesc.toString());
            LOG.error("Unsupport join condition relation, support equal.", exception);
            throw exception;
        }
    }
    
    private boolean isSimpleJoin(BinaryExpressionDesc bdesc)
    {
        return bdesc.getBexpression().getType().equals(ExpressionOperator.EQUAL);
    }
    
    private boolean isInSchema(String value, Schema schema)
    {
        if (StringUtils.isEmpty(value))
        {
            return false;
        }
        
        if (value.equals(schema.getId()))
        {
            return true;
        }
        
        if (value.equals(schema.getName()))
        {
            return true;
        }
        
        if (!StringUtils.isEmpty(schema.getStreamName()) && schema.getStreamName().equalsIgnoreCase(value))
        {
            return true;
        }
        
        return false;
    }
}
