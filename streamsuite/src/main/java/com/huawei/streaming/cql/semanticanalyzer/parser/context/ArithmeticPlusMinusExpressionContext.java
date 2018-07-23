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

import java.util.Iterator;
import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.BinaryExpression;
import com.huawei.streaming.cql.executor.BinaryExpressionRegistry;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.BinaryExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * 算数加减运算表达式解析内容
 * 
 */
public class ArithmeticPlusMinusExpressionContext extends BaseExpressionParseContext
{
    private List<String> operator;
    
    //ArithmeticStarExpressionContext
    private List<BaseExpressionParseContext> starExpressions;
    
    /**
     * <默认构造函数>
     */
    public ArithmeticPlusMinusExpressionContext()
    {
        operator = Lists.newArrayList();
        starExpressions = Lists.newArrayList();
    }
    
    public List<String> getOperator()
    {
        return operator;
    }
    
    public void setOperator(List<String> operator)
    {
        this.operator = operator;
    }
    
    public List<BaseExpressionParseContext> getStarExpressions()
    {
        return starExpressions;
    }
    
    public void setStarExpressions(List<BaseExpressionParseContext> starExpressions)
    {
        this.starExpressions = starExpressions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(starExpressions.get(0));
        for (int i = 0; i < operator.size(); i++)
        {
            sb.append(" " + operator.get(i) + " ").append(starExpressions.get(i + 1).toString());
        }
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpressions(walker, starExpressions);
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
            starExpressions.set(index, replacedContext);
        }
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = Lists.newArrayList();
        for (int i = 0; i < starExpressions.size(); i++)
        {
            BaseExpressionParseContext child = starExpressions.get(i);
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
        if (starExpressions.size() == 1)
        {
            return starExpressions.get(0).createExpressionDesc(getSchemas());
        }
        
        return createBinaryExpression(getSchemas());
    }
    
    /**
     * 创建binary表达式
     *  logicExpressionAnd (KW_OR logicExpressionAnd)*
     * 从后往前计算表达式
     */
    private ExpressionDescribe createBinaryExpression(List<Schema> schemas)
        throws SemanticAnalyzerException
    {
        
        //表达式操作符
        //计算顺序是按照从左向右的顺序操作的，但是解析是从右向左解析的，所以这里的迭代器需要反转
        Iterator<String> operatorIterator = Lists.reverse(operator).iterator();
        if (!operatorIterator.hasNext())
        {
            return null;
        }
        
        BinaryExpression bexp = BinaryExpressionRegistry.getBinaryExpressionByName(operatorIterator.next());
        BinaryExpressionDesc bexpdesc = new BinaryExpressionDesc(bexp);
        
        List<BaseExpressionParseContext> rightArguments = Lists.newLinkedList();
        rightArguments.addAll(starExpressions);
        BaseExpressionParseContext rightArgument = rightArguments.remove(rightArguments.size() - 1);
        
        bexpdesc.getArgExpressions().add(createBinaryExpressionDesc(rightArguments, schemas, operatorIterator));
        bexpdesc.getArgExpressions().add(rightArgument.createExpressionDesc(schemas));
        return bexpdesc;
    }
    
    private ExpressionDescribe createBinaryExpressionDesc(List<BaseExpressionParseContext> leftArguments,
        List<Schema> schemas, Iterator<String> operatorIterator)
        throws SemanticAnalyzerException
    {
        if (leftArguments.size() == 1)
        {
            return leftArguments.get(0).createExpressionDesc(schemas);
        }
        
        if (!operatorIterator.hasNext())
        {
            return leftArguments.get(0).createExpressionDesc(schemas);
        }
        
        BinaryExpression bexp =
            BinaryExpressionRegistry.getBinaryExpressionByName(operatorIterator.next());
        BaseExpressionParseContext rightArgument = leftArguments.remove(leftArguments.size() - 1);
        BinaryExpressionDesc bexpdesc = new BinaryExpressionDesc(bexp);
        
        bexpdesc.getArgExpressions().add(createBinaryExpressionDesc(leftArguments, schemas, operatorIterator));
        bexpdesc.getArgExpressions().add(rightArgument.createExpressionDesc(schemas));
        return bexpdesc;
    }
}
