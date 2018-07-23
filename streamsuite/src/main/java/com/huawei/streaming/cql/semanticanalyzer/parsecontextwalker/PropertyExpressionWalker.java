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

import com.huawei.streaming.cql.semanticanalyzer.parser.context.AtomExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FieldExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * 遍历寻找propertyvalue表达式
 * 
 */
public class PropertyExpressionWalker implements ParseContextWalker
{
    private String streamName;
    
    private String columnName;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean walk(ParseContext parseContext)
    {
        if (!(parseContext instanceof FieldExpressionContext))
        {
            return false;
        }
        FieldExpressionContext fContext = (FieldExpressionContext)parseContext;
        
        if (!(fContext.getAtomExpression() instanceof AtomExpressionContext))
        {
            return false;
        }
        
        AtomExpressionContext atom = (AtomExpressionContext)fContext.getAtomExpression();
        if (atom.getColumnName() == null)
        {
            return false;
        }
        
        streamName = fContext.getStreamNameOrAlias();
        columnName = atom.getColumnName();
        return true;
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public String getColumnName()
    {
        return columnName;
    }
}
