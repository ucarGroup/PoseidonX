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

package com.huawei.streaming.cql.builder.physicoptimizer;

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.AggregateOperator;
import com.huawei.streaming.api.opereators.FunctorOperator;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.ExecutorUtils;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.FunctionExpressionWalker;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.BaseExpressionParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectClauseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;

/**
 * 
 * 将比较简单的聚合算子转换为Filter或者functor算子
 * 
 */
public class AggregateConverter implements Optimizer
{
    private List<Operator> operators;
    
    private List<AggregateOperator> simpleAggregaters = new ArrayList<AggregateOperator>();;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Application optimize(Application app)
        throws ApplicationBuildException
    {
        operators = app.getOperators();
        checkOperators();
        convertOperators(app);
        return null;
    }
    
    private void checkOperators()
        throws ApplicationBuildException
    {
        for (Operator op : operators)
        {
            if (checkOperator(op))
            {
                simpleAggregaters.add((AggregateOperator)op);
            }
        }
    }
    
    private void convertOperators(Application app)
    {
        for (AggregateOperator op : simpleAggregaters)
        {
            FunctorOperator fop = convert(op, app);
            for (int i = 0; i < operators.size(); i++)
            {
                if (operators.get(i) == op)
                {
                    operators.set(i, fop);
                }
            }
        }
    }
    
    private FunctorOperator convert(AggregateOperator op, Application app)
    {
        String newOperatorName = BuilderUtils.renameOperatorName(op.getId(),"Functor");
        FunctorOperator fop = new FunctorOperator(newOperatorName, op.getParallelNumber());
        fop.setOutputExpression(op.getOutputExpression());
        fop.setFilterExpression(op.getFilterBeforeAggregate());


        List<OperatorTransition> fromTransitions = ExecutorUtils.getTransitonsByToId(op.getId(), app.getOpTransition());
        List<OperatorTransition> toTransitions = ExecutorUtils.getTransitonsByFromId(op.getId(), app.getOpTransition());

        for(OperatorTransition transition : fromTransitions)
        {
            transition.setToOperatorId(newOperatorName);
        }

        for(OperatorTransition transition : toTransitions)
        {
            transition.setFromOperatorId(newOperatorName);
        }

        return fop;
    }
    
    /**
     * 检查一个算子是不是可以进行转换
     * 
     * 1、是一个aggreagte算子
     * 2、是一个简单的aggregate算子
     *  即不包含：
     *      orderby、limit、window、groupby
     *      select表达式中不能包含聚合函数
     *      
     *   可以有filterbeforeaggreagte，outputexpression   
     * 
     */
    private boolean checkOperator(Operator op)
        throws SemanticAnalyzerException
    {
        if (!(op instanceof AggregateOperator))
        {
            return false;
        }
        
        AggregateOperator aop = (AggregateOperator)op;
        if (aop.getGroupbyExpression() != null || aop.getOrderBy() != null)
        {
            return false;
        }
        if (aop.getLimit() != null || aop.getWindow() != null)
        {
            return false;
        }
        
        if (isHasAggregate(aop.getOutputExpression()))
        {
            return false;
        }
        
        if (aop.getFilterBeforeAggregate() != null && isHasAggregate(aop.getFilterBeforeAggregate()))
        {
            return false;
        }
        
        return true;
    }
    
    private boolean isHasAggregate(String outputExpression)
        throws SemanticAnalyzerException
    {
        IParser parser = ParserFactory.createSelectClauseParser();
        SelectClauseContext parseContext = (SelectClauseContext)parser.parse(outputExpression);
        for (SelectItemContext selectItem : parseContext.getSelectItems())
        {
            if (isContainsUDAF(selectItem.getExpression().getExpression()))
            {
                return true;
            }
        }
        return false;
    }
    
    private boolean isContainsUDAF(BaseExpressionParseContext parseContext)
        throws SemanticAnalyzerException
    {
        if (parseContext == null)
        {
            return false;
        }
        
        FunctionExpressionWalker walker = new FunctionExpressionWalker();
        parseContext.walk(walker);
        return walker.isContainsUDAF();
    }
}
