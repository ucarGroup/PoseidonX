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

import org.antlr.v4.runtime.DefaultErrorStrategy;
import org.antlr.v4.runtime.NoViableAltException;
import org.antlr.v4.runtime.Parser;
import org.antlr.v4.runtime.Token;
import org.antlr.v4.runtime.TokenStream;
import org.antlr.v4.runtime.misc.Interval;
import org.antlr.v4.runtime.misc.NotNull;

/**
 * CQL语法错误处理
 * <p/>
 * 日志打印级别均为debug级别是因为在发生语法错误的时候，这些接口都会被调用到
 * 所以只要保留异常堆栈即可，其他的日志均可以忽略。
 * 交给上一层parser去处理
 *
 */
public class CQLErrorStrategy extends DefaultErrorStrategy
{
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void reportNoViableAlternative(@NotNull Parser recognizer, @NotNull NoViableAltException e)
    {
        TokenStream tokens = recognizer.getInputStream();
        String input;
        if (tokens instanceof TokenStream)
        {
            if (e.getStartToken().getType() == Token.EOF)
                input = "<EOF>";
            else
                input = getText(tokens, e.getStartToken(), e.getOffendingToken());
        }
        else
        {
            input = "<unknown input>";
        }
        String msg = "no viable alternative at input " + escapeWSAndQuote(input);
        recognizer.notifyErrorListeners(e.getOffendingToken(), msg, e);
    }
    
    @NotNull
    private String getText(TokenStream tokens, Token start, Token stop)
    {
        if (start != null && stop != null)
        {
            return getText(tokens, Interval.of(start.getTokenIndex(), stop.getTokenIndex()));
        }
        
        return "";
    }
    
    @NotNull
    private String getText(TokenStream tokens, Interval interval)
    {
        int start = interval.a;
        int stop = interval.b;
        if (start < 0 || stop < 0)
            return "";
        
        if (stop >= tokens.size())
            stop = tokens.size() - 1;
        
        StringBuilder buf = new StringBuilder();
        for (int i = start; i <= stop; i++)
        {
            Token t = tokens.get(i);
            if (t.getType() == Token.EOF)
                break;
            buf.append(t.getText());
            if (i != stop)
            {
                buf.append(" ");
            }
        }
        return buf.toString();
    }
}
