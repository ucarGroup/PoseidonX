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
 * isnull、like、between、in表达式解析内容
 * 
 */
public class IsNullLikeBetweenInExpressionContext extends BaseExpressionParseContext
{
    //BinaryExpressionContext
    private BaseExpressionParseContext left;
    
    //NullConditionContext
    private BaseExpressionParseContext isNullExpression;
    
    //ExpressionLikeContext
    private BaseExpressionParseContext like;
    
    //ExpressionBetweenContext
    private BaseExpressionParseContext between;
    
    //ExpressionInContext
    private BaseExpressionParseContext in;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        if (isNullExpression != null)
        {
            return left.toString() + " IS " + isNullExpression.toString();
        }
        
        if (like != null)
        {
            return left.toString() + " " + like.toString();
        }
        
        if (between != null)
        {
            return left.toString() + " " + between.toString();
        }
        
        if (in != null)
        {
            return left.toString() + " " + in.toString();
        }
        
        return left.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, isNullExpression);
        walkExpression(walker, left);
        walkExpression(walker, like);
        walkExpression(walker, between);
        walkExpression(walker, in);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        
        walkChildAndReplaceLeft(replacer);
        walkChildAndReplaceIsNull(replacer);
        walkChildAndReplaceLike(replacer);
        walkChildAndReplaceBetween(replacer);
        walkChildAndReplaceIn(replacer);
    }
    
    private void walkChildAndReplaceIn(ParseContextReplacer replacer)
    {
        if (in != null)
        {
            if (replacer.isChildsReplaceable(in))
            {
                in = replacer.createReplaceParseContext();
            }
            else
            {
                in.walkChildAndReplace(replacer);
            }
        }
        
    }
    
    private void walkChildAndReplaceBetween(ParseContextReplacer replacer)
    {
        if (between != null)
        {
            if (replacer.isChildsReplaceable(between))
            {
                between = replacer.createReplaceParseContext();
            }
            else
            {
                between.walkChildAndReplace(replacer);
            }
        }
        
    }
    
    private void walkChildAndReplaceLike(ParseContextReplacer replacer)
    {
        if (like != null)
        {
            if (replacer.isChildsReplaceable(like))
            {
                like = replacer.createReplaceParseContext();
            }
            else
            {
                like.walkChildAndReplace(replacer);
            }
        }
        
    }
    
    private void walkChildAndReplaceIsNull(ParseContextReplacer replacer)
    {
        if (isNullExpression != null)
        {
            if (replacer.isChildsReplaceable(isNullExpression))
            {
                isNullExpression = replacer.createReplaceParseContext();
            }
            else
            {
                isNullExpression.walkChildAndReplace(replacer);
            }
        }
        
    }
    
    private void walkChildAndReplaceLeft(ParseContextReplacer replacer)
    {
        if (left != null)
        {
            if (replacer.isChildsReplaceable(left))
            {
                left = replacer.createReplaceParseContext();
            }
            else
            {
                left.walkChildAndReplace(replacer);
            }
        }
        
    }
    
    public BaseExpressionParseContext getLeft()
    {
        return left;
    }
    
    public void setLeft(BaseExpressionParseContext left)
    {
        this.left = left;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        if (isNullExpression != null)
        {
            return createIsNullExpressionDesc();
        }
        
        if (between != null)
        {
            return createBetweenExpressionDesc();
        }
        
        if (like != null)
        {
            return createLikeExpressionDesc();
        }
        
        if (in != null)
        {
            return createInExpressionDesc();
        }
        
        return left.createExpressionDesc(getSchemas());
    }
    
    private ExpressionDescribe createInExpressionDesc()
        throws SemanticAnalyzerException
    {
        in.setLeftExpression(left);
        return in.createExpressionDesc(getSchemas());
    }
    
    private ExpressionDescribe createLikeExpressionDesc()
        throws SemanticAnalyzerException
    {
        like.setLeftExpression(left);
        return like.createExpressionDesc(getSchemas());
    }
    
    private ExpressionDescribe createBetweenExpressionDesc()
        throws SemanticAnalyzerException
    {
        between.setLeftExpression(left);
        return between.createExpressionDesc(getSchemas());
    }
    
    private ExpressionDescribe createIsNullExpressionDesc()
        throws SemanticAnalyzerException
    {
        isNullExpression.setLeftExpression(left);
        return isNullExpression.createExpressionDesc(getSchemas());
    }
    
    public BaseExpressionParseContext getIsNullExpression()
    {
        return isNullExpression;
    }
    
    public void setIsNullExpression(BaseExpressionParseContext isNullExpression)
    {
        this.isNullExpression = isNullExpression;
    }
    
    public BaseExpressionParseContext getLike()
    {
        return like;
    }
    
    public void setLike(BaseExpressionParseContext like)
    {
        this.like = like;
    }
    
    public BaseExpressionParseContext getBetween()
    {
        return between;
    }
    
    public void setBetween(BaseExpressionParseContext between)
    {
        this.between = between;
    }
    
    public BaseExpressionParseContext getIn()
    {
        return in;
    }
    
    public void setIn(BaseExpressionParseContext in)
    {
        this.in = in;
    }
}
