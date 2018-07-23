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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.NullExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * Null条件语法解析内容
 * 
 */
public class NullConditionContext extends BaseExpressionParseContext
{
    private boolean isNot = false;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return isNot ? "NOT NULL" : "NULL";
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        NullExpressionDesc exp = new NullExpressionDesc(!isNot);
        exp.setExpression(getLeftExpression().createExpressionDesc(getSchemas()));
        return exp;
    }
    
    public boolean isNot()
    {
        return isNot;
    }
    
    public void setNot(boolean isnot)
    {
        this.isNot = isnot;
    }
    
}
