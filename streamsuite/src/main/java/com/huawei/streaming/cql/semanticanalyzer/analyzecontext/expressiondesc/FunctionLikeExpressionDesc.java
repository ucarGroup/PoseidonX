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

import com.huawei.streaming.cql.executor.expressioncreater.FunctionLikeExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 
 * Like表达式
 * 
 */
@ExpressionCreatorAnnotation(FunctionLikeExpressionCreator.class)
public class FunctionLikeExpressionDesc implements ExpressionDescribe
{
    /**
     * 待判断的like的表达收
     */
    private ExpressionDescribe likeProperty;
    
    /**
     * 是否包含not关键字
     * 即是否是 not between
     */
    private boolean isContainsNotExpression = false;
    
    /**
     * 待匹配的字符串
     */
    private ExpressionDescribe likeStringExpression;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String res = likeProperty.toString();
        res = isContainsNotExpression ? res + " not" : res;
        res += " like " + likeStringExpression.toString();
        return res;
    }
    
    public boolean isContainsNotExpression()
    {
        return isContainsNotExpression;
    }
    
    public void setContainsNotExpression(boolean iscontainsnotexpression)
    {
        this.isContainsNotExpression = iscontainsnotexpression;
    }
    
    public ExpressionDescribe getLikeProperty()
    {
        return likeProperty;
    }
    
    public void setLikeProperty(ExpressionDescribe likeProperty)
    {
        this.likeProperty = likeProperty;
    }
    
    public ExpressionDescribe getLikeStringExpression()
    {
        return likeStringExpression;
    }
    
    public void setLikeStringExpression(ExpressionDescribe likeStringExpression)
    {
        this.likeStringExpression = likeStringExpression;
    }
    
}
