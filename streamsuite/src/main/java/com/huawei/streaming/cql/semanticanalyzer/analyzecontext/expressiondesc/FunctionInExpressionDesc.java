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

import java.util.List;

import com.google.common.base.Joiner;
import com.huawei.streaming.cql.executor.expressioncreater.FunctionInExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * In表达式描述内容
 * 
 */
@ExpressionCreatorAnnotation(FunctionInExpressionCreator.class)
public class FunctionInExpressionDesc implements ExpressionDescribe
{
    /**
     * 待判断的in的表达收
     */
    private ExpressionDescribe inProperty;
    
    /**
     * 是否包含not关键字
     * 即是否是 not in
     */
    private boolean isContainsNotExpression = false;
    
    /**
     * in括号中的参数，都是常量表达式
     */
    private List<ExpressionDescribe> args;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" " + inProperty.toString());
        if (isContainsNotExpression)
        {
            sb.append(" not");
        }
        sb.append(" in ( ");
        sb.append(Joiner.on(",").join(args));
        sb.append(" ) ");
        return sb.toString();
    }
    
    public ExpressionDescribe getInProperty()
    {
        return inProperty;
    }
    
    public void setInProperty(ExpressionDescribe inProperty)
    {
        this.inProperty = inProperty;
    }
    
    public List<ExpressionDescribe> getArgs()
    {
        return args;
    }
    
    public void setArgs(List<ExpressionDescribe> args)
    {
        this.args = args;
    }
    
    public boolean isContainsNotExpression()
    {
        return isContainsNotExpression;
    }
    
    public void setContainsNotExpression(boolean iscontainsnotexpression)
    {
        this.isContainsNotExpression = iscontainsnotexpression;
    }
    
}
