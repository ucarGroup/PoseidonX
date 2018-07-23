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

import java.util.Locale;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionLikeExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * like表达式解析内容
 * 
 */
public class ExpressionLikeContext extends BaseExpressionParseContext
{
    
    private boolean isNotLike = false;
    
    private String likeOperator;
    
    private BaseExpressionParseContext likeExpression;
    
    public boolean isNotLike()
    {
        return isNotLike;
    }
    
    public void setNotLike(boolean isnotLike)
    {
        this.isNotLike = isnotLike;
    }
    
    public String getLikeOperator()
    {
        return likeOperator;
    }
    
    public void setLikeOperator(String likeOperator)
    {
        this.likeOperator = likeOperator;
    }
    
    public BaseExpressionParseContext getLikeExpression()
    {
        return likeExpression;
    }
    
    public void setLikeExpression(BitExpressionContext likeExpression)
    {
        this.likeExpression = likeExpression;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (isNotLike)
        {
            sb.append("NOT ");
        }
        sb.append(likeOperator.toUpperCase(Locale.US) + " ");
        sb.append(likeExpression.toString());
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, likeExpression);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(likeExpression))
        {
            likeExpression = replacer.createReplaceParseContext();
        }
        else
        {
            likeExpression.walkChildAndReplace(replacer);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        FunctionLikeExpressionDesc expdesc = new FunctionLikeExpressionDesc();
        expdesc.setLikeProperty(getLeftExpression().createExpressionDesc(getSchemas()));
        expdesc.setContainsNotExpression(isNotLike);
        expdesc.setLikeStringExpression(likeExpression.createExpressionDesc(getSchemas()));
        return expdesc;
    }
}
