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

package com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer;

import java.util.List;

import com.google.common.collect.Lists;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.BaseExpressionParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.SelectItemContext;

/**
 * having语法分析内容表达式替换
 * 
 */
public class HavingParseContextReplacer implements ParseContextReplacer
{
    private SelectItemContext selectItemParseContext;
    
    private List<ParseContext> sameExpressions;
    
    private BaseExpressionParseContext replacedExpressionContext;
    
    /**
     * <默认构造函数>
     */
    public HavingParseContextReplacer(SelectItemContext selectItem, BaseExpressionParseContext replaceExpression)
    {
        selectItemParseContext = selectItem;
        replacedExpressionContext = replaceExpression;
        sameExpressions = Lists.newArrayList();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isChildsReplaceable(BaseExpressionParseContext parseContext)
    {
        if (selectItemParseContext == null)
        {
            return false;
        }
        
        if (parseContext == null)
        {
            return false;
        }
        
        /*
         * 1、判断表达式是否相等
         * 2、判断别名是否相等
         */
        
        boolean result = checkExpressions(parseContext);
        
        if (result)
        {
            return result;
        }
        
        return checkAlia(parseContext);
    }
    
    private boolean checkAlia(BaseExpressionParseContext parseContext)
    {
        if (selectItemParseContext.getExpression().getAlias() != null)
        {
            for (String alia : selectItemParseContext.getExpression().getAlias().getAlias())
            {
                if (alia.equals(parseContext.toString()))
                {
                    sameExpressions.add(parseContext);
                    return true;
                }
            }
        }
        return false;
    }
    
    private boolean checkExpressions(BaseExpressionParseContext parseContext)
    {
        if (selectItemParseContext.getExpression().getExpression() != null)
        {
            if (selectItemParseContext.getExpression().getExpression().toString().equals(parseContext.toString()))
            {
                sameExpressions.add(parseContext);
                return true;
            }
        }
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public BaseExpressionParseContext createReplaceParseContext()
    {
        return replacedExpressionContext;
    }
}
