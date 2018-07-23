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

import com.google.common.collect.Lists;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.BinaryExpression;
import com.huawei.streaming.cql.executor.BinaryExpressionRegistry;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.NullExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 关系表达式解析内容
 * 
 */
public class RelationExpressionContext extends BaseExpressionParseContext
{
    private String relationOperator;
    
    private BaseExpressionParseContext bitExpression;
    
    public String getRelationOperator()
    {
        return relationOperator;
    }
    
    public void setRelationOperator(String relationOperator)
    {
        this.relationOperator = relationOperator;
    }
    
    public BaseExpressionParseContext getBitExpression()
    {
        return bitExpression;
    }
    
    public void setBitExpression(BaseExpressionParseContext bitExpression)
    {
        this.bitExpression = bitExpression;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return relationOperator + " " + bitExpression.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, bitExpression);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(bitExpression))
        {
            bitExpression = replacer.createReplaceParseContext();
        }
        else
        {
            bitExpression.walkChildAndReplace(replacer);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        ExpressionDescribe bitExp = bitExpression.createExpressionDesc(getSchemas());
        ExpressionDescribe leftExp = getLeftExpression().createExpressionDesc(getSchemas());
        
        if (bitExpression.toString().trim().equalsIgnoreCase("null"))
        {
            NullExpressionDesc nullExp = null;
            if (relationOperator.equals("==") || relationOperator.equals("="))
            {
                nullExp = new NullExpressionDesc(true);
            }
            
            if (relationOperator.equals("!=") || relationOperator.equals("<>"))
            {
                nullExp = new NullExpressionDesc(false);
            }
            
            if (nullExp != null)
            {
                nullExp.setExpression(leftExp);
                return nullExp;
            }
            
        }
        
        BinaryExpression bexp = BinaryExpressionRegistry.getBinaryExpressionByName(relationOperator);
        BinaryExpressionDesc exp = new BinaryExpressionDesc(bexp);
        exp.setArgExpressions(Lists.newArrayList(leftExp, bitExp));
        return exp;
    }
}
