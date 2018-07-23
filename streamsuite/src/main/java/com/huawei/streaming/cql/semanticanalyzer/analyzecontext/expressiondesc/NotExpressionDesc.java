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

import com.huawei.streaming.cql.executor.expressioncreater.NotExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * not表达式解析内容
 * 
 */
@ExpressionCreatorAnnotation(NotExpressionCreator.class)
public class NotExpressionDesc implements ExpressionDescribe
{
    
    /**
     * 二元表达式信息
     */
    private ExpressionDescribe exp;
    
    /**
     * <默认构造函数>
     */
    public NotExpressionDesc(ExpressionDescribe expression)
    {
        super();
        this.exp = expression;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "not (" + exp.toString() + ")";
    }
    
    public ExpressionDescribe getExp()
    {
        return exp;
    }
    
    public void setExp(ExpressionDescribe exp)
    {
        this.exp = exp;
    }
}
