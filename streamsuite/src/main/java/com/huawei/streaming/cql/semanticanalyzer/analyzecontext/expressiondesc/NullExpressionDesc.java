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

import com.huawei.streaming.cql.executor.expressioncreater.NullExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 空值表达式
 * 
 */
@ExpressionCreatorAnnotation(NullExpressionCreator.class)
public class NullExpressionDesc implements ExpressionDescribe
{
    /**
     * 如果是true, 就是is null
     * 如果是false，就是is not null
     */
    private boolean isNull;
    
    /**
     * 属性为空的表达式
     * is null 只有一个表达式
     */
    private ExpressionDescribe expression;
    
    /**
     * <默认构造函数>
     * 
     */
    public NullExpressionDesc(boolean isNull)
    {
        super();
        this.isNull = isNull;
    }
    
    /**
     * <默认构造函数>
     * 
     */
    public NullExpressionDesc(boolean isNull, ExpressionDescribe expression)
    {
        super();
        this.isNull = isNull;
        this.expression = expression;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        String nullstr = isNull ? " is null" : " is not null";
        return expression.toString() + nullstr;
    }
    
    public boolean isNull()
    {
        return isNull;
    }
    
    public void setNull(boolean isnull)
    {
        this.isNull = isnull;
    }
    
    public ExpressionDescribe getExpression()
    {
        return expression;
    }
    
    public void setExpression(ExpressionDescribe expression)
    {
        this.expression = expression;
    }
    
}
