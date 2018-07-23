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

package com.huawei.streaming.expression;

import java.nio.charset.Charset;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * <like表达式, 其中match_expression 为任意表达式， pattern 为匹配模式字符串信息， 不包含转义符。 [ ESCAPE escape_character ]暂不支持>
 * <match_expression [ NOT ] LIKE pattern [ ESCAPE escape_character ]
 * 参数
 * match_expression 任何字符串数据类型的有效 SQL Server 表达式。
 * patternmatch_expression 中的搜索模式，可以包含下列有效 SQL Server 通配符
 * 
 * 1 % 包含零个或更多字符的任意字符串
 * 示例：WHERE title LIKE '%computer% ' 将查找处于书名任意位置的包含单词 computer 的所有书名。
 * 
 * 2 _（下划线） 任何单个字符
 * 示例：WHERE au_fname LIKE '_ean ' 将查找以 ean 结尾的所有 4 个字母的名字（Dean、Sean 等）。
 * 
 * 3 [] 指定范围中的任何单个字符
 * 示例：WHERE au_lname LIKE '[C-P]arsen ' 将查找以arsen 结尾且以介于 C 与 P 之间的任何单个字符开始的 作者姓氏，例如，Carsen、Larsen、Karsen 等
 * 
 * 4 [^] 不属于指定范围中的任何单个字符，与 [] 相反
 * 示例：WHERE au_lname LIKE 'de[^l]% ' 将查找以 de 开始且其后的字母不为 l 的所有作者的姓氏。>
 * 
 */
public class LikeExpression implements IBooleanExpression
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 385833815116207526L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LikeExpression.class);
    
    public static final Charset CHARSET = Charset.forName("UTF-8");
    
    /**
     * 是否为like， true = like， false = not like
     */
    private boolean isLike;
    
    /**
     * match_expression
     */
    private IExpression matchExpr;
    
    /**
     * pattern
     */
    private String likePattern;
    
    /**
     * 
     * <匹配类型， 参考Hive，后续需要支持like—regex>
     * <功能详细描述>
     * 
     */
    private enum PatternType
    {
        NONE, // "abc"
        BEGIN, // "abc%"
        END, // "%abc"
        MIDDLE, // "%abc%"
        COMPLEX, // all other cases, such as "ab%c_de"
    }
    
    private PatternType type = PatternType.NONE;
    
    private String simplePattern;
    
    private byte[] simplePatternBytes;
    
    private int simplePatternBytesLength;

    /**
     * <默认构造函数>
     *@param matchExpr 待匹配表达式
     *@param pattern   匹配字符串
     *@param isLike    like or notlike
     *@throws StreamingException 表达式构建异常 
     */
    public LikeExpression(IExpression matchExpr, String pattern, boolean isLike)
        throws StreamingException
    {
        if (matchExpr == null || matchExpr.getType() != String.class)
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_LIKE_STRING);
            LOG.error(ErrorCode.SEMANTICANALYZE_LIKE_STRING.getFullMessage(), exception);
            throw exception;
        }
        
        if (pattern == null || pattern.isEmpty())
        {
            StreamingException exception = new StreamingException(ErrorCode.SEMANTICANALYZE_LIKE_STRING);
            LOG.error(ErrorCode.SEMANTICANALYZE_LIKE_STRING.getFullMessage(), exception);
            throw exception;
        }
        
        this.matchExpr = matchExpr;
        this.likePattern = pattern;
        this.isLike = isLike;
        
        validate();
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (null == theEvent)
        {
            throw new RuntimeException("IEvent is null!");
        }
        
        Object matchValue = matchExpr.evaluate(theEvent);
        if (matchValue == null)
        {
            return null;
        }
        
        Boolean result = match(matchValue);
        if (result == null)
        {
            return null;
        }
        
        if (isLike)
        {
            return result;
        }
        else
        {
            return !result;
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public Object evaluate(IEvent[] eventsPerStream)
    {
        if (null == eventsPerStream || 0 == eventsPerStream.length)
        {
            LOG.error("Streams events are null.");
            throw new RuntimeException("Streams events are null.");
        }
        
        Object matchValue = matchExpr.evaluate(eventsPerStream);
        if (matchValue == null)
        {
            return null;
        }
        
        Boolean result = match(matchValue);
        if (result == null)
        {
            return null;
        }
        
        if (isLike)
        {
            return result;
        }
        else
        {
            return !result;
        }
    }
    
    /** {@inheritDoc} */
    @Override
    public Class< ? > getType()
    {
        return Boolean.class;
    }
    
    private void validate()
    {
        //TODO 未处理转义符
        int length = this.likePattern.length();
        int beginIndex = 0;
        int endIndex = length;
        this.type = PatternType.NONE;
        
        for (int i = 0; i < length; i++)
        {
            char n = likePattern.charAt(i);
            
            if (n == '%')
            {
                if (i == 0)
                {
                    type = PatternType.END;
                    beginIndex = 1;
                }
                else if (i < length - 1)
                {
                    type = PatternType.COMPLEX;
                    return;
                }
                else
                {
                    if (type == PatternType.END)
                    {
                        type = PatternType.MIDDLE;
                        endIndex = length - 1;
                    }
                    else
                    {
                        type = PatternType.BEGIN;
                        endIndex = length - 1;
                    }
                }
            }
            else if (n == '_' || n == '[')
            {
                type = PatternType.COMPLEX;
                return;
            }
        }
        
        simplePattern = likePattern.substring(beginIndex, endIndex);
        simplePatternBytes = simplePattern.getBytes(CHARSET);
        simplePatternBytesLength = simplePatternBytes.length;
    }
    
    private Boolean match(Object matchValue)
    {
        String matchStr = (String)matchValue;
        byte[] matchStrBytes = matchStr.getBytes(CHARSET);
        int matchStrBytesLength = matchStrBytes.length;
        
        if (type == PatternType.COMPLEX)
        {
            Pattern p = Pattern.compile(likePatternToRegex());
            Matcher m = p.matcher(matchStr);
            return m.matches();
        }
        else
        {
            int start = 0;
            int end = matchStrBytesLength;
            
            if (end < simplePatternBytesLength)
            {
                return false;
            }
            
            switch (type)
            {
                case BEGIN:
                    end = simplePatternBytesLength;
                    break;
                case END:
                    start = end - simplePatternBytesLength;
                    break;
                case NONE:
                    if (simplePatternBytesLength != matchStrBytesLength)
                    {
                        return false;
                    }
                    break;
                case MIDDLE:
                    return midMatch(matchStr, start, end);
                default:
                    break;
            }
            
            byte[] b = new byte[end - start];
            System.arraycopy(matchStrBytes, start, b, 0, end - start);
            if (new String(b, CHARSET).equals(simplePattern))
            {
                return true;
            }
        }
        
        return false;
    }
    
    private String likePatternToRegex()
    {
        //TODO 未处理转义符
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < likePattern.length(); i++)
        {
            // Make a special case for "\\_" and "\\%"
            char n = likePattern.charAt(i);
            
            if (n == '_')
            {
                sb.append(".");
            }
            else if (n == '%')
            {
                sb.append(".*");
            }
            else
            {
                /*sb.append(Pattern.quote(Character.toString(n)));*/
                sb.append(Character.toString(n));
            }
        }
        return sb.toString();
    }
    
    private boolean midMatch(String matchStr, int start, int end)
    {
        byte[] byteStr = matchStr.getBytes(CHARSET);
        boolean match = false;
        
        for (int i = start; i < (end - simplePatternBytesLength + 1) && (!match); i++)
        {
            match = true;
            
            for (int j = 0; j < simplePatternBytesLength; j++)
            {
                if (byteStr[i + j] != simplePatternBytes[j])
                {
                    match = false;
                    break;
                }
            }
        }
        
        return match;
    }
    
    public IExpression getMatchExpr()
    {
        return matchExpr;
    }
}
