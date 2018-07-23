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
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.BinaryExpression;
import com.huawei.streaming.cql.executor.BinaryExpressionRegistry;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 
 * 逻辑表达式语Or语法解析内容
 * 
 */
public class LogicExpressionOrContext extends BaseExpressionParseContext
{
    //LogicExpressionAndContext
    private List<BaseExpressionParseContext> expressions;
    
    /**
     * <默认构造函数>
     */
    public LogicExpressionOrContext()
    {
        expressions = Lists.newArrayList();
    }
    
    public List<BaseExpressionParseContext> getExpressions()
    {
        return expressions;
    }
    
    public void setExpression(List<BaseExpressionParseContext> exps)
    {
        this.expressions = exps;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return Joiner.on(" OR ").join(expressions);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpressions(walker, expressions);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = foundIndexsInChilds(replacer);
        replace(replacedIndex, replacer);
    }
    
    private void replace(List<Integer> replacedIndex, ParseContextReplacer replacer)
    {
        BaseExpressionParseContext replacedContext = replacer.createReplaceParseContext();
        for (Integer index : replacedIndex)
        {
            expressions.set(index, replacedContext);
        }
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = Lists.newArrayList();
        for (int i = 0; i < expressions.size(); i++)
        {
            BaseExpressionParseContext child = expressions.get(i);
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
        if (expressions.size() == 1)
        {
            return expressions.get(0).createExpressionDesc(getSchemas());
        }
        
        return createBinaryExpression();
    }
    
    /**
     * 创建binary表达式
     *  logicExpressionAnd (KW_OR logicExpressionAnd)*
     * 从后往前计算表达式
     */
    private ExpressionDescribe createBinaryExpression()
        throws SemanticAnalyzerException
    {
        BinaryExpression bexp = BinaryExpressionRegistry.getBinaryExpressionByName("or");
        BinaryExpressionDesc bexpdesc = new BinaryExpressionDesc(bexp);
        
        List<BaseExpressionParseContext> rightArguments = Lists.newLinkedList();
        rightArguments.addAll(expressions);
        BaseExpressionParseContext leftArgument = rightArguments.remove(0);
        
        bexpdesc.getArgExpressions().add(leftArgument.createExpressionDesc(getSchemas()));
        bexpdesc.getArgExpressions().add(createBinaryExpressionDesc(rightArguments));
        return bexpdesc;
    }
    
    private ExpressionDescribe createBinaryExpressionDesc(List<BaseExpressionParseContext> rightArguments)
        throws SemanticAnalyzerException
    {
        if (rightArguments.size() == 1)
        {
            return rightArguments.get(0).createExpressionDesc(getSchemas());
        }
        
        BaseExpressionParseContext leftArgument = rightArguments.remove(0);
        BinaryExpression bexp = BinaryExpressionRegistry.getBinaryExpressionByName("or");
        BinaryExpressionDesc bexpdesc = new BinaryExpressionDesc(bexp);
        bexpdesc.getArgExpressions().add(leftArgument.createExpressionDesc(getSchemas()));
        bexpdesc.getArgExpressions().add(createBinaryExpressionDesc(rightArguments));
        return bexpdesc;
    }
    
}
