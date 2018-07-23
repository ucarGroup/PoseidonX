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
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionWhenExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * when表达式解析内容
 * 
 */
public class WhenExpressionContext extends BaseExpressionParseContext
{
    //CaseWhenBodyWhenBodyContext
    private List<BaseExpressionParseContext> whens;
    
    //CaseWhenBodyThenBodyContext
    private List<BaseExpressionParseContext> thens;
    
    //CaseWhenElseContext
    private BaseExpressionParseContext elseExpression;
    
    /**
     * <默认构造函数>
     */
    public WhenExpressionContext()
    {
        whens = Lists.newArrayList();
        thens = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE");
        for (int i = 0; i < whens.size(); i++)
        {
            sb.append(" " + whens.get(i).toString());
            sb.append(" ");
            sb.append(thens.get(i).toString());
        }
        if (elseExpression != null)
        {
            sb.append(" " + elseExpression.toString());
        }
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, elseExpression);
        walkExpressions(walker, whens);
        walkExpressions(walker, thens);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        walkChildAndReplaceEnd(replacer);
        walkChildAndReplaceExpressions(replacer, whens);
        walkChildAndReplaceExpressions(replacer, thens);
        
    }
    
    private void walkChildAndReplaceExpressions(ParseContextReplacer replacer,
        List<BaseExpressionParseContext> expressions)
    {
        List<Integer> replacedIndex = foundIndexsInChilds(replacer, expressions);
        replace(replacedIndex, replacer, expressions);
    }
    
    private void walkChildAndReplaceEnd(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(elseExpression))
        {
            elseExpression = replacer.createReplaceParseContext();
        }
        else
        {
            elseExpression.walkChildAndReplace(replacer);
        }
    }
    
    private void replace(List<Integer> replacedIndex, ParseContextReplacer replacer,
        List<BaseExpressionParseContext> expressions)
    {
        BaseExpressionParseContext replacedContext = replacer.createReplaceParseContext();
        for (Integer index : replacedIndex)
        {
            expressions.set(index, replacedContext);
        }
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer,
        List<BaseExpressionParseContext> expressions)
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
        List<Pair<ExpressionDescribe, ExpressionDescribe>> whenThens = Lists.newArrayList();
        
        for (int i = 0; i < whens.size(); i++)
        {
            ExpressionDescribe when = whens.get(i).createExpressionDesc(getSchemas());
            ExpressionDescribe then = thens.get(i).createExpressionDesc(getSchemas());
            whenThens.add(new Pair<ExpressionDescribe, ExpressionDescribe>(when, then));
        }
        FunctionWhenExpressionDesc expdesc = new FunctionWhenExpressionDesc();
        expdesc.setWhenThens(whenThens);
        
        if (elseExpression != null)
        {
            expdesc.setElseExpression(elseExpression.createExpressionDesc(getSchemas()));
        }
        return expdesc;
    }
    
    public List<BaseExpressionParseContext> getWhens()
    {
        return whens;
    }
    
    public void setWhens(List<BaseExpressionParseContext> whens)
    {
        this.whens = whens;
    }
    
    public List<BaseExpressionParseContext> getThens()
    {
        return thens;
    }
    
    public void setThens(List<BaseExpressionParseContext> thens)
    {
        this.thens = thens;
    }
    
    public BaseExpressionParseContext getElseExpression()
    {
        return elseExpression;
    }
    
    public void setElseExpression(BaseExpressionParseContext elseExpression)
    {
        this.elseExpression = elseExpression;
    }
}
