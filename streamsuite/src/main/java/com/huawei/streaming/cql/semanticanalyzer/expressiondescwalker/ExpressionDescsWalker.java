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

package com.huawei.streaming.cql.semanticanalyzer.expressiondescwalker;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ConstExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionBetweenExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionCaseExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionInExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionLikeExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionPreviousDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionWhenExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.JoinExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.NullExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.StreamAliasDesc;

/**
 * 遍历所有的表达式描述内容，获取需要的表达式
 *
 */
public class ExpressionDescsWalker
{
    private static final Logger LOG = LoggerFactory.getLogger(ExpressionDescsWalker.class);
    
    private ExpressionDescGetterStrategy strategy;
    
    /**
     * <默认构造函数>
     *
     */
    public ExpressionDescsWalker(ExpressionDescGetterStrategy getStrategy)
    {
        this.strategy = getStrategy;
    }
    
    /**
     * 递归表达式中的Previous 函数
     *
     */
    public void found(ExpressionDescribe expression, List<ExpressionDescribe> expressionContainer)
        throws ApplicationBuildException
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
            expressionContainer.add(expression);
            return;
        }
        
        walkBinaryExpression(expression, expressionContainer);
        walkFunctions(expression, expressionContainer);
        walkOthers(expression, expressionContainer);
    }
    
    private void walkBinaryExpression(ExpressionDescribe expression, List<ExpressionDescribe> expressionContainer)
        throws ApplicationBuildException
    {
        if (expression instanceof BinaryExpressionDesc)
        {
            BinaryExpressionDesc opexp = (BinaryExpressionDesc)expression;
            for (ExpressionDescribe exp : opexp.getArgExpressions())
            {
                found(exp, expressionContainer);
            }
        }
    }
    
    private void walkOthers(ExpressionDescribe expression, List<ExpressionDescribe> expressionContainer)
        throws ApplicationBuildException
    {
        if (expression instanceof ConstExpressionDesc)
        {
            return;
        }
        
        if (expression instanceof JoinExpressionDesc)
        {
            return;
        }
        
        if (expression instanceof NullExpressionDesc)
        {
            NullExpressionDesc opexp = (NullExpressionDesc)expression;
            found(opexp.getExpression(), expressionContainer);
        }
        
        if (expression instanceof PropertyValueExpressionDesc)
        {
            return;
        }
        
        if (expression instanceof StreamAliasDesc)
        {
            return;
        }
    }
    
    private void walkFunctions(ExpressionDescribe expression, List<ExpressionDescribe> expressionContainer)
        throws ApplicationBuildException
    {
        walkCaseWhens(expression, expressionContainer);
        if (expression instanceof FunctionExpressionDesc)
        {
            FunctionExpressionDesc opexp = (FunctionExpressionDesc)expression;
            for (ExpressionDescribe exp : opexp.getArgExpressions())
            {
                found(exp, expressionContainer);
            }
        }
        
        walkFunctionExpression(expression, expressionContainer);
    }
    
    private void walkFunctionExpression(ExpressionDescribe expression, List<ExpressionDescribe> expressionContainer)
        throws ApplicationBuildException
    {
        if (expression instanceof FunctionBetweenExpressionDesc)
        {
            FunctionBetweenExpressionDesc opexp = (FunctionBetweenExpressionDesc)expression;
            found(opexp.getLeftExpression(), expressionContainer);
            found(opexp.getRightExpression(), expressionContainer);
            found(opexp.getBetweenProperty(), expressionContainer);
        }
        
        if (expression instanceof FunctionInExpressionDesc)
        {
            FunctionInExpressionDesc opexp = (FunctionInExpressionDesc)expression;
            found(opexp.getInProperty(), expressionContainer);
            for (ExpressionDescribe exp : opexp.getArgs())
            {
                found(exp, expressionContainer);
            }
        }
        
        if (expression instanceof FunctionLikeExpressionDesc)
        {
            FunctionLikeExpressionDesc opexp = (FunctionLikeExpressionDesc)expression;
            found(opexp.getLikeProperty(), expressionContainer);
            found(opexp.getLikeStringExpression(), expressionContainer);
        }
        
        if (expression instanceof FunctionPreviousDesc)
        {
            FunctionPreviousDesc opexp = (FunctionPreviousDesc)expression;
            found(opexp.getPreviouNumber(), expressionContainer);
            for (ExpressionDescribe exp : opexp.getPreviouCols())
            {
                found(exp, expressionContainer);
            }
        }
    }
    
    private void walkCaseWhens(ExpressionDescribe expression, List<ExpressionDescribe> expressionContainer)
        throws ApplicationBuildException
    {
        if (expression instanceof FunctionCaseExpressionDesc)
        {
            FunctionCaseExpressionDesc opexp = (FunctionCaseExpressionDesc)expression;
            found(opexp.getCasePropertyExpression(), expressionContainer);
            found(opexp.getElseExpression(), expressionContainer);
            for (Pair<ExpressionDescribe, ExpressionDescribe> exps : opexp.getWhenThens())
            {
                found(exps.getFirst(), expressionContainer);
                found(exps.getSecond(), expressionContainer);
            }
        }
        
        if (expression instanceof FunctionWhenExpressionDesc)
        {
            FunctionWhenExpressionDesc opexp = (FunctionWhenExpressionDesc)expression;
            found(opexp.getElseExpression(), expressionContainer);
            for (Pair<ExpressionDescribe, ExpressionDescribe> exps : opexp.getWhenThens())
            {
                found(exps.getFirst(), expressionContainer);
                found(exps.getSecond(), expressionContainer);
            }
        }
    }
}
