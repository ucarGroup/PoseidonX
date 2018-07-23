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

package com.huawei.streaming.cql.executor;

import com.huawei.streaming.expression.ExpressionOperator;
import com.huawei.streaming.expression.IExpression;

/**
 * 二元表达式
 * 
 */
public class BinaryExpression
{
    /**
     * 表达式的缩写
     * 也是表达式的别名
     */
    private String describe;
    
    /**
     * 表达式类型，换算成底层的执行器中的枚举类型
     */
    private ExpressionOperator type;
    
    /**
     * 要进行表达式计算的类
     */
    private Class< ? extends IExpression> expressClass;
    
    /**
     * <默认构造函数>
     */
    public BinaryExpression(String describe, ExpressionOperator type, Class< ? extends IExpression> expressClass)
    {
        super();
        this.describe = describe;
        this.type = type;
        this.expressClass = expressClass;
    }
    
    public String getDescribe()
    {
        return describe;
    }
    
    public void setDescribe(String describe)
    {
        this.describe = describe;
    }
    
    public ExpressionOperator getType()
    {
        return type;
    }
    
    public void setType(ExpressionOperator type)
    {
        this.type = type;
    }
    
    public Class< ? extends IExpression> getExpressClass()
    {
        return expressClass;
    }
    
    public void setExpressClass(Class< ? extends IExpression> expressClass)
    {
        this.expressClass = expressClass;
    }
    
}
