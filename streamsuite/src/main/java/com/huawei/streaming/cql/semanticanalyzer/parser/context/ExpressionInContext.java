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

import java.util.ArrayList;
import java.util.List;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionInExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * in表达式解析内容
 * 
 */
public class ExpressionInContext extends BaseExpressionParseContext
{
    private boolean isNot = false;
    
    private List<BaseExpressionParseContext> expressions;
    
    /**
     * <默认构造函数>
     */
    public ExpressionInContext()
    {
        expressions = new ArrayList<BaseExpressionParseContext>();
    }
    
    public boolean isNot()
    {
        return isNot;
    }
    
    public void setNot(boolean isnot)
    {
        this.isNot = isnot;
    }
    
    public List<BaseExpressionParseContext> getExpressions()
    {
        return expressions;
    }
    
    public void setExpressions(List<BaseExpressionParseContext> expressions)
    {
        this.expressions = expressions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        if (isNot)
        {
            sb.append(" NOT ");
        }
        
        sb.append(" IN ");
        sb.append(" ( ");
        sb.append(Joiner.on(", ").join(expressions));
        sb.append(" ) ");
        return sb.toString();
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
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        List<ExpressionDescribe> args = Lists.newArrayList();
        for (BaseExpressionParseContext exp : expressions)
        {
            args.add(exp.createExpressionDesc(getSchemas()));
        }
        
        FunctionInExpressionDesc function = new FunctionInExpressionDesc();
        function.setInProperty(getLeftExpression().createExpressionDesc(getSchemas()));
        function.setArgs(args);
        function.setContainsNotExpression(isNot);
        return function;
    }
}
