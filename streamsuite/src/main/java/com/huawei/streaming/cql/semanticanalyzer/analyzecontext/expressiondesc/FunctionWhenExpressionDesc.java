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

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.executor.expressioncreater.FunctionWhenExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 
 * when表达式
 * 
 */
@ExpressionCreatorAnnotation(FunctionWhenExpressionCreator.class)
public class FunctionWhenExpressionDesc implements ExpressionDescribe
{
    private List<Pair<ExpressionDescribe, ExpressionDescribe>> whenThens;
    
    private ExpressionDescribe elseExpression;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        /*
         * case when key<'90' then 2 when key = '90' then 3 when key > '90'  then 5 else 4 end
         * (TOK_SELEXPR (TOK_FUNCTION when (< (TOK_STREAM_OR_COL key) '90') 2 
         *      (= (TOK_STREAM_OR_COL key) '90') 3 (> (TOK_STREAM_OR_COL key) '90') 5 4))
         */
        StringBuilder sb = new StringBuilder("case");
        
        for (int i = 0; i < whenThens.size(); i++)
        {
            Pair<ExpressionDescribe, ExpressionDescribe> whenthen = whenThens.get(i);
            
            sb.append(" when " + whenthen.getFirst().toString());
            sb.append(" then " + whenthen.getSecond().toString());
        }
        
        if (elseExpression != null)
        {
            sb.append(" else " + elseExpression.toString());
        }
        sb.append(" end ");
        
        return sb.toString();
    }
    
    public List<Pair<ExpressionDescribe, ExpressionDescribe>> getWhenThens()
    {
        return whenThens;
    }
    
    public void setWhenThens(List<Pair<ExpressionDescribe, ExpressionDescribe>> whenThens)
    {
        this.whenThens = whenThens;
    }
    
    public ExpressionDescribe getElseExpression()
    {
        return elseExpression;
    }
    
    public void setElseExpression(ExpressionDescribe elseExpression)
    {
        this.elseExpression = elseExpression;
    }
    
}
