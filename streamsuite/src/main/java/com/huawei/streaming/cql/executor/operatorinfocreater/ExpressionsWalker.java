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

package com.huawei.streaming.cql.executor.operatorinfocreater;

import java.util.List;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.AggregateGroupedExpression;
import com.huawei.streaming.expression.BetweenExpression;
import com.huawei.streaming.expression.CaseSearchedExpression;
import com.huawei.streaming.expression.CaseSimpleExpression;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.ExprIsNullExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.InExpression;
import com.huawei.streaming.expression.LikeExpression;
import com.huawei.streaming.expression.MethodExpression;
import com.huawei.streaming.expression.NotExpression;
import com.huawei.streaming.expression.OperatorBasedExpression;
import com.huawei.streaming.expression.PreviousExpression;
import com.huawei.streaming.expression.PropertyIsNullExpression;
import com.huawei.streaming.expression.PropertyValueExpression;

/**
 * 遍历所有的表达式，获取需要的表达式
 * 
 */
public class ExpressionsWalker
{
    private ExpressionGetterStrategy strategy;
    
    /**
     * <默认构造函数>
     */
    public ExpressionsWalker(ExpressionGetterStrategy getStrategy)
    {
        this.strategy = getStrategy;
    }
    
    /**
     * 递归表达式中的Previous 函数
     */
    public void found(IExpression expression, List<IExpression> expressionContainer)
        throws ExecutorException
    {
        if (expressionContainer == null || null == expression)
        {
            return;
        }
        
        /*
         * 执行策略
         */
        if (strategy.isEqual(expression))
        {
            expressionContainer.add((IExpression)expression);
            return;
        }
        
        walkMethodExpression(expression, expressionContainer);
        walkCaseWhenExpression(expression, expressionContainer);
        
        /*
         * 包含between，like，in之类的表达式
         */
        walkBooleanExpressions(expression, expressionContainer);
        
        /*
         * 包含了ArithmeticExpression、LogicExpression、RelationExpression
         */
        walkOperatorBasedExpression(expression, expressionContainer);
        
        /*
         * 其他都是一些常量或者属性表达式，是不包含聚合表达式的
         */
        walkSimpleExpression(expression, expressionContainer);
        return;
    }
    
    private void walkSimpleExpression(IExpression expression, List<IExpression> expressionContainer)
        throws ExecutorException
    {
        if (expression instanceof AggregateExpression)
        {
            AggregateExpression opexp = (AggregateExpression)expression;
            found(opexp.getAggArgExpression(), expressionContainer);
            found(opexp.getAggArgFilterExpression(), expressionContainer);
        }
        
        if (expression instanceof AggregateGroupedExpression)
        {
            return;
        }
        
        if (expression instanceof ConstExpression)
        {
            return;
        }
        
        if (expression instanceof PreviousExpression)
        {
            found(((PreviousExpression)expression).getProExpr(), expressionContainer);
            found(((PreviousExpression)expression).getIndexExpr(), expressionContainer);
        }
        
        if (expression instanceof PropertyIsNullExpression)
        {
            return;
        }
        
        if (expression instanceof PropertyValueExpression)
        {
            return;
        }
    }
    
    private void walkOperatorBasedExpression(IExpression expression, List<IExpression> expressionContainer)
        throws ExecutorException
    {
        if (expression instanceof OperatorBasedExpression)
        {
            OperatorBasedExpression opexp = (OperatorBasedExpression)expression;
            found(opexp.getLeftExpr(), expressionContainer);
            found(opexp.getRightExpr(), expressionContainer);
        }
    }
    
    private void walkMethodExpression(IExpression expression, List<IExpression> expressionContainer)
        throws ExecutorException
    {
        if (expression instanceof MethodExpression)
        {
            for (IExpression exp : ((MethodExpression)expression).getExpr())
            {
                found(exp, expressionContainer);
            }
        }
    }
    
    private void walkCaseWhenExpression(IExpression expression, List<IExpression> expressionContainer)
        throws ExecutorException
    {
        if (expression instanceof CaseSearchedExpression)
        {
            CaseSearchedExpression opexp = (CaseSearchedExpression)expression;
            for (Pair<IExpression, IExpression> exps : opexp.getWhenThenExprs())
            {
                found(exps.getFirst(), expressionContainer);
                found(exps.getSecond(), expressionContainer);
            }
            
            if (opexp.getOptionalElseExpr() != null)
            {
                found(opexp.getOptionalElseExpr(), expressionContainer);
            }
        }
        
        if (expression instanceof CaseSimpleExpression)
        {
            CaseSimpleExpression opexp = (CaseSimpleExpression)expression;
            found(opexp.getInputExpr(), expressionContainer);
            
            for (Pair<IExpression, IExpression> exps : opexp.getWhenThenExprs())
            {
                found(exps.getFirst(), expressionContainer);
                found(exps.getSecond(), expressionContainer);
            }
            if (opexp.getOptionalElseExpr() != null)
            {
                found(opexp.getOptionalElseExpr(), expressionContainer);
            }
        }
    }
    
    private void walkBooleanExpressions(IExpression expression, List<IExpression> previousExpression)
        throws ExecutorException
    {
        if (expression instanceof BetweenExpression)
        {
            BetweenExpression opexp = (BetweenExpression)expression;
            found(opexp.getLeftExpr(), previousExpression);
            found(opexp.getHighExpr(), previousExpression);
            found(opexp.getLowExpr(), previousExpression);
        }
        
        if (expression instanceof ExprIsNullExpression)
        {
            ExprIsNullExpression opexp = (ExprIsNullExpression)expression;
            found(opexp.getExpr(), previousExpression);
        }
        
        if (expression instanceof InExpression)
        {
            InExpression opexp = (InExpression)expression;
            found(opexp.getLeftExpr(), previousExpression);
            
            for (IExpression exp : opexp.getRightExprs())
            {
                found(exp, previousExpression);
            }
        }
        
        if (expression instanceof LikeExpression)
        {
            LikeExpression opexp = (LikeExpression)expression;
            found(opexp.getMatchExpr(), previousExpression);
        }
        
        if (expression instanceof NotExpression)
        {
            NotExpression opexp = (NotExpression)expression;
            found(opexp.getInnerExpr(), previousExpression);
        }
    }
    
}
