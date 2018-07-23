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

package com.huawei.streaming.cql.semanticanalyzer.parser;

import org.antlr.v4.runtime.ANTLRInputStream;
import org.antlr.v4.runtime.CharStream;

/**
 * 忽略大小写的antlr处理规则
 * antlr中默认是区分大小写的，但是CQL语句中对关键词是不区分的
 * 所以要在进行分析的时候，忽略关键词的大小写。
 * 
 */
class ANTLRIgnoreCaseStringStream extends ANTLRInputStream
{
    
    /**
     * <默认构造函数>
     * 
     */
    public ANTLRIgnoreCaseStringStream(String input)
    {
        super(input);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public int LA(int i)
    {
        
        int returnChar = super.LA(i);
        if (returnChar == CharStream.EOF)
        {
            return returnChar;
        }
        else if (returnChar == 0)
        {
            return returnChar;
        }
        
        return Character.toUpperCase((char)returnChar);
    }
    
}
