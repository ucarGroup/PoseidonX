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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionBetweenExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * between表达式解析内容
 * 
 */
public class ExpressionBetweenContext extends BaseExpressionParseContext
{
    private boolean isNot = false;
    
    //ExpressionBetweenMinValueContext
    private BaseExpressionParseContext minValue;
    
    //ExpressionBetweenMaxValueContext
    private BaseExpressionParseContext maxValue;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        /*
         * expressionBetween
            :   identifierNot? KW_BETWEEN expressionBetweenMinValue KW_AND expressionBetweenMaxValue 
            ;
         */
        StringBuilder sb = new StringBuilder();
        if (isNot)
        {
            sb.append(" NOT ");
        }
        sb.append(" BETWEEN ");
        sb.append(minValue.toString());
        sb.append(" AND ");
        sb.append(maxValue.toString());
        return sb.toString();
    }
    
    public boolean isNot()
    {
        return isNot;
    }
    
    public void setNot(boolean isnot)
    {
        this.isNot = isnot;
    }
    
    public BaseExpressionParseContext getMinValue()
    {
        return minValue;
    }
    
    public void setMinValue(ExpressionBetweenMinValueContext minValue)
    {
        this.minValue = minValue;
    }
    
    public BaseExpressionParseContext getMaxValue()
    {
        return maxValue;
    }
    
    public void setMaxValue(ExpressionBetweenMaxValueContext maxValue)
    {
        this.maxValue = maxValue;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, minValue);
        walkExpression(walker, maxValue);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(minValue))
        {
            minValue = replacer.createReplaceParseContext();
        }
        else
        {
            minValue.walkChildAndReplace(replacer);
        }
        if (replacer.isChildsReplaceable(maxValue))
        {
            maxValue = replacer.createReplaceParseContext();
        }
        else
        {
            maxValue.walkChildAndReplace(replacer);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        FunctionBetweenExpressionDesc between = new FunctionBetweenExpressionDesc();
        between.setBetweenProperty(getLeftExpression().createExpressionDesc(getSchemas()));
        between.setContainsNotExpression(isNot);
        between.setLeftExpression(minValue.createExpressionDesc(getSchemas()));
        between.setRightExpression(maxValue.createExpressionDesc(getSchemas()));
        return between;
    }
    
}
