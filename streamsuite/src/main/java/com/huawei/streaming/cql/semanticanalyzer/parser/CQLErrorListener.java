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

import java.util.List;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.cql.exception.ParseException;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 语法解析异常处理
 *
 */
public class CQLErrorListener extends BaseErrorListener
{
    
    private static final Logger LOG = LoggerFactory.getLogger(CQLErrorStrategy.class);
    
    private static final String SYMBOL_EOF = "<EOF>";
    
    private static final int MIN_SIZE_FOR_TOKENS = 2;
    
    private ParseException parserException;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void syntaxError(Recognizer<?, ?> recognizer, Object offendingSymbol, int line, int charPositionInLine,
        String msg, RecognitionException e)
    {
        if (parserException != null)
        {
            return;
        }
        
        String errorSymbol = getOffendingSymbol(recognizer, offendingSymbol, charPositionInLine);
        parserException =
            new ParseException(
                ErrorCode.SEMANTICANALYZE_PARSE_ERROR,
                "You have an error in your CQL syntax; check the manual that corresponds to your Streaming version for the right syntax to use near '"
                    + errorSymbol + "' at line " + line + ":" + charPositionInLine);
        LOG.error(parserException.getMessage(), parserException);
    }
    
    /**
     * 获取保存的语法异常
     *
     */
    public ParseException getRecException()
    {
        return parserException;
    }
    
    /**
     * 获取错误单词总入口
     */
    private String getOffendingSymbol(Recognizer<?, ?> recognizer, Object offendingSymbol, int charPositionInLine)
    {
        if (null != offendingSymbol)
        {
            return getOffendingSymbolWithHint(recognizer, offendingSymbol);
        }
        
        String inputSentence = recognizer.getInputStream().toString();
        return Strings.isNullOrEmpty(inputSentence) ? "" : getOffendingSymbolWithoutHint(inputSentence, charPositionInLine);
    }
    
    /**
     * 在语法解析器可以定位到错误单词的基础下获取错误单词
     */
    private String getOffendingSymbolWithHint(Recognizer<?, ?> recognizer, Object offendingSymbol)
    {
        Token token = (Token)offendingSymbol;
        String tokenText = token.getText();
        if (tokenText.equals(SYMBOL_EOF))
        {
            List<Token> allTokens = ((org.antlr.v4.runtime.CommonTokenStream)recognizer.getInputStream()).getTokens();
            int tokensCount = allTokens.size();
            return (tokensCount < MIN_SIZE_FOR_TOKENS) ? "" : allTokens.get(tokensCount - MIN_SIZE_FOR_TOKENS)
                .getText();
        }
        return tokenText;
    }
    /**
     * 从偏移量处向前找第一个非空格的位置
     */
    private int findPositionNotSpaceBackForward(String input, int offset)
    {
        String subStr = input.substring(0, offset);
        for (int i = offset - 1; i >= 0; i--)
        {
            if (subStr.charAt(i) != ' ')
            {
                return i;
            }
        }
        return 0;
    }
    
    /**
     * 在语法解析器未能定位到错误单词的情况下使用错误偏移量找出附近的单词
     */
    private String getOffendingSymbolWithoutHint(String input, int charPositionInLine)
    {
        int start = 0;
        int end = 0;
        int offset = charPositionInLine;
        
        if (offset > input.length())
        {
            offset = input.length() - 1;
        }
        
        if (offset < 0)
        {
            offset = 0;
        }
        
        //如果错误偏移量定位到的字符是空格，那么需要进行特殊处理
        if (input.charAt(offset) == ' ')
        {
            offset = findPositionNotSpaceBackForward(input, offset);
        }
        
        //从偏移量向前找最后出现的空格
        start = input.lastIndexOf(" ", offset);
        //从偏移量向后找第一个出现的空格
        end = input.indexOf(" ", offset);
        
        start = (start == -1) ? 0 : (start + 1);
        
        end = (end == -1) ? input.length() : end;   
        
        return input.substring(start, end);
        
    }
}
