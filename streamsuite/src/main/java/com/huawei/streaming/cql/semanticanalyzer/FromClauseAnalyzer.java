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

package com.huawei.streaming.cql.semanticanalyzer;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateStreamAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.FromClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.SelectClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.DatasourceBodyDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescFactory;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.JoinExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.StreamAliasDesc;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ColumnNameTypeListContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.DataSourceBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FilterBeforeWindowContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FromClauseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FromSourceContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.JoinRightBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.OnConditionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.StreamBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.WindowSourceContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * from子句语法分析
 * 
 */
public class FromClauseAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(FromClauseAnalyzer.class);
    
    private FromClauseAnalyzeContext analyzeContext;
    
    private FromClauseContext fromContext;
    
    /**
     * <默认构造函数>
     */
    public FromClauseAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        fromContext = (FromClauseContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        parseFromContext();
        return analyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        analyzeContext = new FromClauseAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return analyzeContext;
    }
    
    private void parseFromContext()
        throws SemanticAnalyzerException
    {
        /*
         * 在多流Join或者combine的时候，要先计算最右边的流
         */
        FromSourceContext left = fromContext.getSourceContext().getLeftStream();
        List<JoinRightBodyContext> rights = fromContext.getSourceContext().getJoinRightBody();
        if (rights.size() == 0)
        {
            parseFromSource(left);
            return;
        }
        
        parseJoinContext();
    }
    
    private void parseJoinContext()
        throws SemanticAnalyzerException
    {
        multiJoinValidate();
        JoinExpressionDesc joinDescs = parseJoin();
        analyzeContext.setJoinexpression(joinDescs);
        if (isCombine())
        {
            combineValidate();
            parseComine();
        }
    }
    
    private void parseComine()
        throws SemanticAnalyzerException
    {
        for (ExpressionContext expcontexxt : fromContext.getCombine().getCombineExpressions())
        {
            ExpressionDescribe exp =
                ExpressionDescFactory.createExpressionDesc(expcontexxt, analyzeContext.getInputSchemas());
            if (!(exp instanceof PropertyValueExpressionDesc))
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_COMBINE_SIMPLE_EXPRESSION, exp.toString());
                LOG.error("Unsupport expression type in combine clause.", exception);
                
                throw exception;
            }
            PropertyValueExpressionDesc pexp = (PropertyValueExpressionDesc)exp;
            String streamName = pexp.getSchemaId();
            analyzeContext.getCombineConditions().put(streamName, pexp);
        }
    }
    
    private JoinExpressionDesc parseJoin()
        throws SemanticAnalyzerException
    {
        FromSourceContext left = fromContext.getSourceContext().getLeftStream();
        List<JoinRightBodyContext> rights = fromContext.getSourceContext().getJoinRightBody();
        return parseJoin(left, rights);
    }
    
    private JoinExpressionDesc parseJoin(FromSourceContext left, List<JoinRightBodyContext> rights)
        throws SemanticAnalyzerException
    {
        if (rights.size() == 1)
        {
            return createJoinDesc(left, rights.get(0));
        }
        else
        {
            List<JoinRightBodyContext> newRights = Lists.newArrayList();
            newRights.addAll(rights);
            newRights.remove(0);
            JoinExpressionDesc rightJoinDesc = parseJoin(rights.get(0), Lists.newArrayList(newRights));
            return createJoinDesc(left, rights.get(0), rightJoinDesc);
        }
    }
    
    private JoinExpressionDesc parseJoin(JoinRightBodyContext left, List<JoinRightBodyContext> rights)
        throws SemanticAnalyzerException
    {
        if (rights.size() == 1)
        {
            return createJoinDesc(left.getRightStream(), rights.get(0));
        }
        else
        {
            List<JoinRightBodyContext> newRights = Lists.newArrayList();
            newRights.addAll(rights);
            newRights.remove(0);
            JoinExpressionDesc rightJoinDesc = parseJoin(rights.get(0), Lists.newArrayList(newRights));
            return createJoinDesc(left.getRightStream(), rights.get(0), rightJoinDesc);
        }
    }
    
    private JoinExpressionDesc createJoinDesc(FromSourceContext left, JoinRightBodyContext right)
        throws SemanticAnalyzerException
    {
        JoinExpressionDesc joinContext = new JoinExpressionDesc(right.getJoinType());
        joinContext.setJointype(right.getJoinType());
        joinContext.setLeftExpression(parseFromSource(left));
        //右侧是整个list的Join之后的结果
        joinContext.setRightExpression(parseFromSource(right.getRightStream()));
        if (right.getOnCondition() != null)
        {
            joinContext.setJoinCondition(parseJoinCondition(right.getOnCondition()));
        }
        return joinContext;
    }
    
    private JoinExpressionDesc createJoinDesc(FromSourceContext left, JoinRightBodyContext right,
        JoinExpressionDesc rightJoinDesc)
        throws SemanticAnalyzerException
    {
        JoinExpressionDesc joinContext = new JoinExpressionDesc(right.getJoinType());
        joinContext.setLeftExpression(parseFromSource(left));
        //右侧是整个list的Join之后的结果
        joinContext.setRightExpression(rightJoinDesc);
        if (right.getOnCondition() != null)
        {
            joinContext.setJoinCondition(parseJoinCondition(right.getOnCondition()));
        }
        return joinContext;
    }
    
    private boolean isCombine()
    {
        return fromContext.getCombine() != null;
    }
    
    private ExpressionDescribe parseFromSource(FromSourceContext from)
        throws SemanticAnalyzerException
    {
        if (from.getStreamBody() != null)
        {
            return parseStreamBody(from.getStreamBody());
        }
        
        return parseDataSourceBody(from.getDataSoruceBody());
    }
    
    private DatasourceBodyDesc parseDataSourceBody(DataSourceBodyContext dataSoruceBody)
    {
        DatasourceBodyDesc desc = new DatasourceBodyDesc();
        desc.setDatasourceName(dataSoruceBody.getDataSourceName());
        desc.setQueryArguments(parseQueryArguments(dataSoruceBody));
        Schema schema = createDataSourceSchema(dataSoruceBody);
        
        analyzeContext.getInputSchemas().add(schema);
        desc.setSchema(schema);
        
        String alias = dataSoruceBody.getAlia();
        alias = alias == null ? schema.getId() : alias;
        analyzeContext.addInputStream(alias);
        return desc;
    }
    
    /*
     * DataSource Schema中
     * schema id为新New出来的
     * schema Name 就是数据源名称
     * streamName就是 数据源别名
     */
    private Schema createDataSourceSchema(DataSourceBodyContext dataSoruceBody)
    {
        Schema schema = new Schema(dataSoruceBody.getDataSourceName());
        schema.setStreamName(dataSoruceBody.getDataSourceName());
        
        ColumnNameTypeListContext columns = dataSoruceBody.getSchemaColumns();
        for (ColumnNameTypeContext column : columns.getColumns())
        {
            schema.addCol(new Column(column.getColumnName(), column.getColumnType().getWrapperClass()));
        }
        
        String alia = dataSoruceBody.getAlia();
        if (alia != null)
        {
            schema.setName(alia);
        }
        return schema;
    }
    
    private List<String> parseQueryArguments(DataSourceBodyContext dataSoruceBody)
    {
        List<ExpressionContext> queryArguments = dataSoruceBody.getQueryarguments();
        List<String> args = Lists.newArrayList();
        for (int i = 0; i < queryArguments.size(); i++)
        {
            args.add(queryArguments.get(i).toString());
        }
        
        return args;
    }
    
    private StreamAliasDesc parseStreamBody(StreamBodyContext from)
        throws SemanticAnalyzerException
    {
        String streamName = null;
        Schema schema = null;
        if (from.getStreamSource().getSubQuery() != null)
        {
            streamName = createNewStreamName();
            schema = parseSubQuery(streamName, from.getStreamSource().getSubQuery());
        }
        else
        {
            streamName = from.getStreamSource().getStreamName();
            schema = getSchemaByName(streamName).cloneSchema();
        }
        
        boolean isUniDirect = from.isUnidirection();
        StreamAliasDesc salias = createStreamAliasDesc(from, streamName, schema);
        parseUnidirection(isUniDirect, salias);
        return salias;
        
    }
    
    /*
     * 检查是否存在多表Join
     * 1、combine不存在
     * 2、不存在两个以上的流
     */
    private void multiJoinValidate()
        throws SemanticAnalyzerException
    {
        List<JoinRightBodyContext> rights = fromContext.getSourceContext().getJoinRightBody();
        if (rights.isEmpty())
        {
            return;
        }
        
        if (!isCombine() && rights.size() > CQLConst.I_2)
        {
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_MULTI_JOIN);
            LOG.error("Too many stream join.", exception);
            
            throw exception;
        }
        
    }
    
    /**
     * combine的校验
     * combine中不允许存在窗口
     */
    private void combineValidate()
        throws SemanticAnalyzerException
    {
        if (analyzeContext.getWindows().size() != 0)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_COMBINE_WINDOW);
            LOG.error("Window in combine clause.", exception);
            
            throw exception;
        }
    }
    
    /**
     * 窗口解析
     * 
     * (TOK_WINDOW keepall)
     * (TOK_WINDOW time_slide (TOK_STREAMPROPLIST (TOK_STREAMPROPERTY 100)))
     * (TOK_WINDOW group_time_slide (TOK_STREAMPROPLIST (TOK_STREAMPROPERTY 100) (TOK_STREAMPROPERTY 'id,name')))
     * (TOK_WINDOW udf (TOK_STREAMPROPLIST (TOK_STREAMPROPERTY 'id' '100') (TOK_STREAMPROPERTY 'ss' 'id,name')))
     * 
     */
    private void parseWindow(WindowSourceContext windowContext, Schema schema, String alias)
        throws SemanticAnalyzerException
    {
        WindowAnalyzer analyzer = new WindowAnalyzer(windowContext);
        analyzer.init(Lists.newArrayList(schema));
        analyzeContext.addWindow(alias, analyzer.analyze().createWindowByParseContext());
    }
    
    /**
     * 由于语义解析的需要，为不存在窗口的流创建null的默认窗口
     */
    private void createDefaultWindow(String alias)
    {
        if (isCombine())
        {
            return;
        }
        analyzeContext.addWindow(alias, null);
    }
    
    private ExpressionDescribe parseJoinCondition(OnConditionContext onConetxt)
        throws SemanticAnalyzerException
    {
        return ExpressionDescFactory.createExpressionDesc(onConetxt.getExpression(), analyzeContext.getInputSchemas());
    }
    
    private void parseUnidirection(boolean isUniDirect, StreamAliasDesc salias)
        throws SemanticAnalyzerException
    {
        if (isUniDirect)
        {
            String uname = salias.getStreamName();
            if (!StringUtils.isEmpty(analyzeContext.getUniDirections()))
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNIDIRECTION_ONLY_ONE);
                LOG.error("Too many stream set unidirection property.", exception);
                
                throw exception;
            }
            if (!StringUtils.isEmpty(salias.getStreamAlias()))
            {
                uname = salias.getStreamAlias();
            }
            analyzeContext.setUniDirections(uname);
        }
    }
    
    private StreamAliasDesc createStreamAliasDesc(StreamBodyContext from, String streamName, Schema schema)
        throws SemanticAnalyzerException
    {
        StreamAliasDesc desc = new StreamAliasDesc();
        desc.setStreamName(streamName);
        FilterBeforeWindowContext filterAfterWinAST = from.getFilterBeforeWindow();
        WindowSourceContext windowAST = from.getWindow();
        String alias = from.getAlias();
        if (alias != null)
        {
            desc.setStreamAlias(alias);
            schema.setName(alias);
        }
        analyzeContext.getInputSchemas().add(schema);
        
        //TODO 在多流Join的时候，会导致有schema找不到
        //比如 S1 join S2 on s1.id = s2.id join s3 on s1.id=s3.id
        //就会先解析右边的S2和S3,这样S1的schema就找不到了
        //所以后期所有的parseContext都要实现walk接口
        alias = alias == null ? streamName : alias;
        
        /*
         * 这里解析window和处理filterbeforewindow的时候，
         * 使用别名，防止出现selfJoin的情况
         */
        analyzeContext.addInputStream(alias);
        if (windowAST != null)
        {
            parseWindow(windowAST, schema, alias);
        }
        else
        {
            createDefaultWindow(alias);
        }
        
        if (filterAfterWinAST != null)
        {
            parseFilterBeforeWindow(alias, filterAfterWinAST);
        }
        return desc;
    }
    
    /**
     * 解析子查询
     * 
     * 从子查询中解析获取Schema信息，解析Schema列类型
     * 
     * 还要根据schema信息模拟出create pipestream解析结果
     * 还有insert into stream的解析结果
     * 
     */
    private Schema parseSubQuery(String subStreamName, SelectStatementContext selectStatement)
        throws SemanticAnalyzerException
    {
        SelectAnalyzeContext subSelectParseContext = analyzeSubQuery(selectStatement);
        Schema schema = getSchemaFromSubQuery(subStreamName, subSelectParseContext);
        createNewInsertContext(subStreamName, subSelectParseContext, schema);
        createNewStreamContext(subStreamName, schema);
        return schema;
    }
    
    private SelectAnalyzeContext analyzeSubQuery(SelectStatementContext selectStatement)
        throws SemanticAnalyzerException
    {
        SemanticAnalyzer subAnalyzer = SemanticAnalyzerFactory.createAnalyzer(selectStatement, getAllSchemas());
        return (SelectAnalyzeContext)subAnalyzer.analyze();
    }
    
    private void createNewStreamContext(String subStreamName, Schema schema)
    {
        CreateStreamAnalyzeContext createStreamContext = new CreateStreamAnalyzeContext();
        createStreamContext.setSchema(schema);
        createStreamContext.setStreamName(subStreamName);
        analyzeContext.getSubQuerySchemas().put(subStreamName, createStreamContext);
    }
    
    private void createNewInsertContext(String subStreamName, SelectAnalyzeContext subSelectParseContext, Schema schema)
    {
        InsertAnalyzeContext ipc = new InsertAnalyzeContext();
        ipc.setSelectContext(subSelectParseContext);
        ipc.setOutputSchema(schema);
        ipc.setOutputStreamName(subStreamName);
        analyzeContext.getSubQueryForStream().put(subStreamName, ipc);
    }
    
    private Schema getSchemaFromSubQuery(String subStreamName, SelectAnalyzeContext subSelectParseContext)
        throws SemanticAnalyzerException
    {
        SelectClauseAnalyzeContext selectClause = subSelectParseContext.getSelectClauseContext();
        Schema outputSchema = selectClause.getOutputSchema().cloneSchema();
        outputSchema.setId(subStreamName);
        outputSchema.setName(subStreamName);
        outputSchema.setStreamName(subStreamName);
        return outputSchema;
    }
    
    private void parseFilterBeforeWindow(String streamName, FilterBeforeWindowContext filterContext)
        throws SemanticAnalyzerException
    {
        if (filterContext == null)
        {
            return;
        }
        
        List<Schema> schemas =
            Lists.newArrayList(BaseAnalyzer.getSchemaByName(streamName, analyzeContext.getInputSchemas()));
        ExpressionDescribe desc = ExpressionDescFactory.createExpressionDesc(filterContext.getExpression(), schemas);
        analyzeContext.addFilterBeforeWindow(streamName, desc);
    }
    
    private String createNewStreamName()
    {
        BuilderUtils bu = DriverContext.getBuilderNameSpace().get();
        return bu.getNextSubQueryName();
    }
}
