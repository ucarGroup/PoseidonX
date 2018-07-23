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
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionCaseExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;

/**
 * case 表达式解析内容
 * 
 */
public class CaseExpressionContext extends BaseExpressionParseContext
{
    private BaseExpressionParseContext head;
    
    //CaseWhenBodyWhenBodyContext
    private List<BaseExpressionParseContext> whens;
    
    //CaseWhenBodyThenBodyContext
    private List<BaseExpressionParseContext> thens;
    
    private BaseExpressionParseContext caseWhenElse;
    
    /**
     * <默认构造函数>
     */
    public CaseExpressionContext()
    {
        whens = Lists.newArrayList();
        thens = Lists.newArrayList();
    }
    
    public BaseExpressionParseContext getHead()
    {
        return head;
    }
    
    public void setHead(BaseExpressionParseContext head)
    {
        this.head = head;
    }
    
    public BaseExpressionParseContext getCaseWhenElse()
    {
        return caseWhenElse;
    }
    
    public void setCaseWhenElse(BaseExpressionParseContext caseWhenElse)
    {
        this.caseWhenElse = caseWhenElse;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CASE ");
        sb.append(head.toString() + " ");
        
        for (int i = 0; i < whens.size(); i++)
        {
            sb.append(whens.get(i).toString());
            sb.append(" ");
            sb.append(thens.get(i).toString());
            sb.append(" ");
        }
        
        if (caseWhenElse != null)
        {
            sb.append(" " + caseWhenElse.toString());
        }
        sb.append(" END ");
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, head);
        walkExpressions(walker, whens);
        walkExpressions(walker, thens);
        walkExpression(walker, caseWhenElse);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        walkChildAndReplaceHead(replacer);
        walkChildAndReplaceExpressions(replacer, whens);
        walkChildAndReplaceExpressions(replacer, thens);
        walkChildAndReplaceEnd(replacer);
    }
    
    private void walkChildAndReplaceEnd(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(caseWhenElse))
        {
            caseWhenElse = replacer.createReplaceParseContext();
        }
        else
        {
            caseWhenElse.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceHead(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(head))
        {
            head = replacer.createReplaceParseContext();
        }
        else
        {
            head.walkChildAndReplace(replacer);
        }
    }
    
    private void walkChildAndReplaceExpressions(ParseContextReplacer replacer,
        List<BaseExpressionParseContext> expressions)
    {
        List<Integer> replacedIndex = foundIndexsInChilds(replacer, expressions);
        replace(replacedIndex, replacer, expressions);
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
        FunctionCaseExpressionDesc expdesc = new FunctionCaseExpressionDesc();
        expdesc.setCasePropertyExpression(head.createExpressionDesc(getSchemas()));
        expdesc.setWhenThens(whenThens);
        if (caseWhenElse != null)
        {
            expdesc.setElseExpression(caseWhenElse.createExpressionDesc(getSchemas()));
        }
        return expdesc;
    }
}
