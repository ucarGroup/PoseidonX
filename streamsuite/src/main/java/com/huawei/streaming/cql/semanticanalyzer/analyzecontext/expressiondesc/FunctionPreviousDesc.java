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

import com.google.common.base.Joiner;
import com.huawei.streaming.cql.executor.expressioncreater.FunctionPreviousExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * previous表达式解析结果
 * 
 */
@ExpressionCreatorAnnotation(FunctionPreviousExpressionCreator.class)
public class FunctionPreviousDesc implements ExpressionDescribe
{
    
    private ConstExpressionDesc previouNumber;
    
    private List<PropertyValueExpressionDesc> previouCols = new ArrayList<PropertyValueExpressionDesc>();;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" previous (");
        sb.append(previouNumber.toString() + ",");
        sb.append(Joiner.on(",").join(previouCols));
        sb.append(" ) ");
        return sb.toString();
    }
    
    public ConstExpressionDesc getPreviouNumber()
    {
        return previouNumber;
    }
    
    public void setPreviouNumber(ConstExpressionDesc previouNumber)
    {
        this.previouNumber = previouNumber;
    }
    
    public List<PropertyValueExpressionDesc> getPreviouCols()
    {
        return previouCols;
    }
    
    public void setPreviouCols(List<PropertyValueExpressionDesc> previouCols)
    {
        this.previouCols = previouCols;
    }
    
}
