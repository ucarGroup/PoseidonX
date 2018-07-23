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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import com.huawei.streaming.cql.executor.expressioncreater.FunctionBetweenExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 
 * between表达式
 * 
 */
@ExpressionCreatorAnnotation(FunctionBetweenExpressionCreator.class)
public class FunctionBetweenExpressionDesc implements ExpressionDescribe
{
    /**
     * 待判断的between的表达式
     */
    private ExpressionDescribe betweenProperty;
    
    /**
     * 是否包含not关键字
     * 即是否是 not between
     */
    private boolean isContainsNotExpression = false;
    
    /**
     * between的左边表达式
     */
    private ExpressionDescribe leftExpression;
    
    /**
     * bewteen的右边表达式
     */
    private ExpressionDescribe rightExpression;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String res = " between " + leftExpression.toString() + " and " + rightExpression.toString();
        
        return isContainsNotExpression ? betweenProperty.toString() + " not" + res : betweenProperty.toString() + res;
    }
    
    public boolean isContainsNotExpression()
    {
        return isContainsNotExpression;
    }
    
    public void setContainsNotExpression(boolean iscontainsnotexpression)
    {
        this.isContainsNotExpression = iscontainsnotexpression;
    }
    
    public ExpressionDescribe getLeftExpression()
    {
        return leftExpression;
    }
    
    public void setLeftExpression(ExpressionDescribe leftExpression)
    {
        this.leftExpression = leftExpression;
    }
    
    public ExpressionDescribe getRightExpression()
    {
        return rightExpression;
    }
    
    public void setRightExpression(ExpressionDescribe rightExpression)
    {
        this.rightExpression = rightExpression;
    }
    
    public ExpressionDescribe getBetweenProperty()
    {
        return betweenProperty;
    }
    
    public void setBetweenProperty(ExpressionDescribe betweenProperty)
    {
        this.betweenProperty = betweenProperty;
    }
    
}
