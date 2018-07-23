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

package com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker;

import java.util.List;

import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * having的表达式遍历
 * 
 */
public class HavingExpressionWalker implements ParseContextWalker
{
    private List<ParseContext> selectExprs;
    
    /**
     * <默认构造函数>
     */
    public HavingExpressionWalker(List<ParseContext> selectExpressions)
    {
        selectExprs = selectExpressions;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean walk(ParseContext parseContext)
    {
        for (ParseContext selectitem : selectExprs)
        {
            if (selectExprs.getClass().getName().equals(parseContext.getClass().getName()))
            {
                if (parseContext.toString().equals(selectitem.toString()))
                {
                    //TODO 在having中找到了匹配的列
                    return true;
                }
            }
        }
        return false;
    }
    
}
