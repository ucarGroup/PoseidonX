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

import com.google.common.collect.Lists;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.BinaryExpression;
import com.huawei.streaming.cql.executor.BinaryExpressionRegistry;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 位操作表达式语法解析内容
 * 
 */
public class BitExpressionContext extends BaseExpressionParseContext
{
    private List<String> bitOperator;
    
    //ArithmeticPlusMinusExpressionContext
    private List<BaseExpressionParseContext> arithmeticExpressions;
    
    /**
     * <默认构造函数>
     */
    public BitExpressionContext()
    {
        bitOperator = Lists.newArrayList();
        arithmeticExpressions = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(arithmeticExpressions.get(0));
        for (int i = 0; i < bitOperator.size(); i++)
        {
            sb.append(" " + bitOperator.get(i) + " ").append(arithmeticExpressions.get(i + 1));
        }
        return sb.toString();
    }
    
    public List<String> getBitOperator()
    {
        return bitOperator;
    }
    
    public void setBitOperator(List<String> bitOperator)
    {
        this.bitOperator = bitOperator;
    }
    
    public List<BaseExpressionParseContext> getArithmeticExpressions()
    {
        return arithmeticExpressions;
    }
    
    public void setArithmeticExpressions(List<BaseExpressionParseContext> arithmeticExpressions)
    {
        this.arithmeticExpressions = arithmeticExpressions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpressions(walker, arithmeticExpressions);
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
            arithmeticExpressions.set(index, replacedContext);
        }
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = Lists.newArrayList();
        for (int i = 0; i < arithmeticExpressions.size(); i++)
        {
            BaseExpressionParseContext child = arithmeticExpressions.get(i);
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
        if (arithmeticExpressions.size() == 1)
        {
            return arithmeticExpressions.get(0).createExpressionDesc(getSchemas());
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
        BinaryExpression bexp =
            BinaryExpressionRegistry.getBinaryExpressionByName(bitOperator.get(arithmeticExpressions.size() - 1));
        BinaryExpressionDesc bexpdesc = new BinaryExpressionDesc(bexp);
        
        List<BaseExpressionParseContext> rightArguments = Lists.newLinkedList();
        rightArguments.addAll(arithmeticExpressions);
        BaseExpressionParseContext rightArgument = rightArguments.remove(rightArguments.size() - 1);
        
        bexpdesc.getArgExpressions().add(createBinaryExpressionDesc(rightArguments));
        bexpdesc.getArgExpressions().add(rightArgument.createExpressionDesc(getSchemas()));
        return bexpdesc;
    }
    
    private ExpressionDescribe createBinaryExpressionDesc(List<BaseExpressionParseContext> leftArguments)
        throws SemanticAnalyzerException
    {
        if (leftArguments.size() == 1)
        {
            return leftArguments.get(0).createExpressionDesc(getSchemas());
        }
        
        BinaryExpression bexp =
            BinaryExpressionRegistry.getBinaryExpressionByName(bitOperator.get(arithmeticExpressions.size() - 1));
        BaseExpressionParseContext rightArgument = leftArguments.remove(leftArguments.size() - 1);
        BinaryExpressionDesc bexpdesc = new BinaryExpressionDesc(bexp);
        
        bexpdesc.getArgExpressions().add(createBinaryExpressionDesc(leftArguments));
        bexpdesc.getArgExpressions().add(rightArgument.createExpressionDesc(getSchemas()));
        return bexpdesc;
    }
}
