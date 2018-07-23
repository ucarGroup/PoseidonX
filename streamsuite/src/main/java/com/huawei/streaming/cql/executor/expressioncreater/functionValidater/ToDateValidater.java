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

package com.huawei.streaming.cql.executor.expressioncreater.functionValidater;

import java.text.SimpleDateFormat;

import com.google.common.base.Strings;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ConstExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.udfs.ToDate;
import com.huawei.streaming.udfs.UDFAnnotation;

/**
 * todate函数参数校验
 * 
 */
public class ToDateValidater implements FunctionValidater
{
    private String fName;;
    
    public ToDateValidater()
    {
        UDFAnnotation annotation = ToDate.class.getAnnotation(UDFAnnotation.class);
        fName = annotation == null ? null : annotation.value();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(String functionName, IExpression[] argumentExpressions,
        FunctionExpressionDesc functionExpressionDesc)
    {
        boolean result = checkIsFunction(functionName, argumentExpressions);
        
        if (result)
        {
            return true;
        }
        
        if (functionExpressionDesc.getArgExpressions().size() != argumentExpressions.length)
        {
            return true;
        }
        
        
        if(!(functionExpressionDesc.getArgExpressions().get(1) instanceof ConstExpressionDesc))
        {
            return true;
        }
        
     
        Object format = ((ConstExpressionDesc)functionExpressionDesc.getArgExpressions().get(1)).getConstValue();
        if(format == null)
        {
            return true;
        }
        
        String timeFormat = format.toString();
        
        try
        {
            new SimpleDateFormat(timeFormat);
        }
        catch (IllegalArgumentException e)
        {
            return false;
        }
        
        return true;
    }
    
    private boolean checkIsFunction(String functionName, IExpression[] argumentExpressions)
    {
        if (Strings.isNullOrEmpty(functionName))
        {
            return true;
        }
        
        if (!fName.equals(functionName))
        {
            return true;
        }
        
        if (argumentExpressions.length != 2)
        {
            return true;
        }
        
        if (argumentExpressions[0].getType() != String.class || argumentExpressions[1].getType() != String.class)
        {
            return true;
        }
        return false;
    }
    
}
