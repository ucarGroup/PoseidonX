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
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * equalrelationexpression解析内容
 * 
 */
public class EqualRelationExpressionContext extends BaseExpressionParseContext
{
    //IsNullLikeInExpressionsContext
    private BaseExpressionParseContext nullLike;
    
    private BaseExpressionParseContext exists;
    
    public BaseExpressionParseContext getExists()
    {
        return exists;
    }
    
    public void setExists(BaseExpressionParseContext exists)
    {
        this.exists = exists;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (nullLike != null)
        {
            return nullLike.toString();
        }
        return exists.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, nullLike);
        walkExpression(walker, exists);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        walkChildAndReplaceNullLike(replacer);
        walkChildAndReplaceExists(replacer);
        
    }
    
    private void walkChildAndReplaceExists(ParseContextReplacer replacer)
    {
        if (exists != null)
        {
            if (replacer.isChildsReplaceable(exists))
            {
                exists = replacer.createReplaceParseContext();
            }
            else
            {
                exists.walkChildAndReplace(replacer);
            }
        }
    }
    
    private void walkChildAndReplaceNullLike(ParseContextReplacer replacer)
    {
        if (nullLike != null)
        {
            if (replacer.isChildsReplaceable(nullLike))
            {
                nullLike = replacer.createReplaceParseContext();
            }
            else
            {
                nullLike.walkChildAndReplace(replacer);
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        if (exists != null)
        {
            return exists.createExpressionDesc(getSchemas());
        }
        
        return nullLike.createExpressionDesc(getSchemas());
    }
    
    public BaseExpressionParseContext getNullLike()
    {
        return nullLike;
    }
    
    public void setNullLike(BaseExpressionParseContext nullLike)
    {
        this.nullLike = nullLike;
    }
    
}
