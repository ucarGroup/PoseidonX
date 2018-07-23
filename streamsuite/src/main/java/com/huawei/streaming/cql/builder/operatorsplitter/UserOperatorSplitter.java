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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.opereators.FilterOperator;
import com.huawei.streaming.api.opereators.FunctionStreamOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.InsertUserOperatorStatementAnalyzeContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 使用自定义算子的拆分
 */
public class UserOperatorSplitter implements Splitter
{
    private static final Logger LOG = LoggerFactory.getLogger(UserOperatorSplitter.class);
    
    private InsertUserOperatorStatementAnalyzeContext context;
    
    private SplitContext result = new SplitContext();
    
    private BuilderUtils buildUtils;
    
    /**
     * <默认构造函数>
     */
    public UserOperatorSplitter(BuilderUtils buildUtils)
    {
        this.buildUtils = buildUtils;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        return parseContext instanceof InsertUserOperatorStatementAnalyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SplitContext split(AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        this.context = (InsertUserOperatorStatementAnalyzeContext)parseContext;
        result.setParseContext(context);
        
        FunctionStreamOperator userOperator =
            new FunctionStreamOperator(buildUtils.getNextOperatorName(context.getOperatorName()), getParallelNumber());
        userOperator.setOperatorClass(context.getOperatorClassName());
        userOperator.setDistributedColumnName(context.getDistributedByColumnName());
        userOperator.setArgs(context.getProperties());
        userOperator.setInputSchema(context.getInputSchmea().cloneSchema());
        userOperator.setOutputSchema(context.getOutputSchema().cloneSchema());
        
        //TODO 重构需求： Filter这些算子名称可以考虑放在builerUtils中作为常量或者作为算子属性
        /*
         * 临时创建一个filter operator用来制作和useroperator中的连线
         * 这个filtreoperator不做其他的事情，在后期的优化器中会移除该算子
         */
        FilterOperator fop = new FilterOperator(buildUtils.getNextOperatorName("Filter"), 1);
        fop.setOutputExpression(createFilterOutputExpression());
        
        OperatorTransition transition = createTransition(fop, userOperator);
        
        result.setOutputStreamName(context.getOutputStreamName());
        result.addOperators(fop);
        result.addOperators(userOperator);
        result.addTransitions(transition);
        return result;
    }
    
    private OperatorTransition createTransition(Operator fromOp, Operator toOp)
        throws ApplicationBuildException
    {
        DistributeType distype = DistributeType.SHUFFLE;
        String disFields = null;
        Schema schema = context.getInputSchmea();
        
        if (context.getDistributedByColumnName() != null)
        {
            if (!validateDistributeColumn(context.getDistributedByColumnName(), schema))
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NO_COLUMN_ALLSTREAM,
                        context.getDistributedByColumnName());
                LOG.error("Cannot find column {} in related streams.", context.getDistributedByColumnName());
                throw exception;
            }
            
            disFields = context.getDistributedByColumnName();
            distype = DistributeType.FIELDS;
        }
        
        return new OperatorTransition(buildUtils.getNextStreamName(), fromOp, toOp, distype, disFields, schema);
    }
    
    private boolean validateDistributeColumn(String distributeColumn, Schema schema)
    {
        for (Column column : schema.getCols())
        {
            if (!StringUtils.isEmpty(column.getName()) && column.getName().equalsIgnoreCase(distributeColumn))
            {
                return true;
            }
            if (!StringUtils.isEmpty(column.getAlias()) && column.getAlias().equalsIgnoreCase(distributeColumn))
            {
                return true;
            }
        }
        return false;
    }
    
    private String createFilterOutputExpression()
        throws SemanticAnalyzerException
    {
        Schema schema = context.getInputSchmea();
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
    
    private Integer getParallelNumber()
    {
        if (context.getParallelNumber() == null)
        {
            return buildUtils.getDefaultParallelNumber();
        }
        else
        {
            return context.getParallelNumber();
        }
    }
    
}
