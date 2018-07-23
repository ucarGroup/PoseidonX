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

import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * binary 表达式解析内容
 * 
 */
public class BinaryExpressionContext extends BaseExpressionParseContext
{
    //BitExpressionContext
    private BaseExpressionParseContext bitExpression;
    
    //RelationExpressionContext
    private List<BaseExpressionParseContext> relationExpression;
    
    /**
     * <默认构造函数>
     */
    public BinaryExpressionContext()
    {
        relationExpression = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(bitExpression.toString());
        if (relationExpression.size() > 0)
        {
            sb.append(" ");
        }
        sb.append(Joiner.on(" ").join(relationExpression));
        return sb.toString();
    }
    
    public BaseExpressionParseContext getBitExpression()
    {
        return bitExpression;
    }
    
    public void setBitExpression(BaseExpressionParseContext bitExpression)
    {
        this.bitExpression = bitExpression;
    }
    
    public List<BaseExpressionParseContext> getRelationExpression()
    {
        return relationExpression;
    }
    
    public void setRelationExpression(List<BaseExpressionParseContext> relationExpression)
    {
        this.relationExpression = relationExpression;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, bitExpression);
        walkExpressions(walker, relationExpression);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        walkChildAndReplaceBitExpression(replacer);
        walkChildAndReplaceRelationExpressions(replacer);
    }
    
    private void walkChildAndReplaceRelationExpressions(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = foundIndexsInChilds(replacer);
        replace(replacedIndex, replacer);
    }
    
    private void walkChildAndReplaceBitExpression(ParseContextReplacer replacer)
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
    
    private void replace(List<Integer> replacedIndex, ParseContextReplacer replacer)
    {
        BaseExpressionParseContext replacedContext = replacer.createReplaceParseContext();
        for (Integer index : replacedIndex)
        {
            relationExpression.set(index, replacedContext);
        }
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = Lists.newArrayList();
        for (int i = 0; i < relationExpression.size(); i++)
        {
            BaseExpressionParseContext child = relationExpression.get(i);
            if (replacer.isChildsReplaceable(child))
            {
                replacedIndex.add(i);
            }
            else
            {
                child.walkChildAndReplace(replacer);
            }
        }
        return replacedIndex;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        if (relationExpression.size() == 0)
        {
            return bitExpression.createExpressionDesc(getSchemas());
        }
        
        return createRelationExpression(getSchemas());
    }
    
    /**
     * 创建binary表达式
     *  logicExpressionAnd (KW_OR logicExpressionAnd)*
     * 从后往前计算表达式
     */
    private ExpressionDescribe createRelationExpression(List<Schema> schemas)
        throws SemanticAnalyzerException
    {
        List<BaseExpressionParseContext> rightArguments = Lists.newLinkedList();
        rightArguments.addAll(relationExpression);
        
        relationExpression.get(0).setLeftExpression(bitExpression);
        for (int i = 1; i < relationExpression.size(); i++)
        {
            relationExpression.get(i).setLeftExpression(relationExpression.get(i - 1));
        }
        
        return relationExpression.get(relationExpression.size() - 1).createExpressionDesc(schemas);
    }
}
