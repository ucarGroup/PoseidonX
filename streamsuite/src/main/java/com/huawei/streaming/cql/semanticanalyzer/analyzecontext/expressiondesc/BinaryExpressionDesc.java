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

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.cql.executor.BinaryExpression;
import com.huawei.streaming.cql.executor.expressioncreater.BinaryExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 二元表达式描述
 * 
 * 包含对逻辑表达式、算术表达式和关系表达式的描述
 * 
 */
@ExpressionCreatorAnnotation(BinaryExpressionCreator.class)
public class BinaryExpressionDesc implements ExpressionDescribe
{
    
    /**
     * 二元表达式信息
     */
    private BinaryExpression bexpression;
    
    /**
     * 二元表达式的参数信息
     */
    private List<ExpressionDescribe> argExpressions = new ArrayList<ExpressionDescribe>();;
    
    /**
     * <默认构造函数>
     */
    public BinaryExpressionDesc(BinaryExpression bexpression)
    {
        super();
        this.bexpression = bexpression;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return "(" + argExpressions.get(0).toString() + " " + bexpression.getType().getDesc() + " "
            + argExpressions.get(1).toString() + ")";
    }
    
    public BinaryExpression getBexpression()
    {
        return bexpression;
    }
    
    public void setBexpression(BinaryExpression bexpression)
    {
        this.bexpression = bexpression;
    }
    
    public List<ExpressionDescribe> getArgExpressions()
    {
        return argExpressions;
    }
    
    public void setArgExpressions(List<ExpressionDescribe> argExpressions)
    {
        this.argExpressions = argExpressions;
    }
    
}
