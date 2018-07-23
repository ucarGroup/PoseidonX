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
package com.huawei.streaming.cql.exception;


import com.google.common.base.Strings;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * CQL通用异常
 *
 */
public class CQLException extends StreamingException
{
    private static final long serialVersionUID = -2141365981910063112L;
    
    /**
     * 摘要长度
     */
    private static final int SUMMARY_LENGTH = 30;
    
    /**
     * 字符串摘要后缀
     */
    private static final String SUMMARY_POSTFIX = "...";
    
    /**
     * 原始的CQL语句，用于在语法解析错误的时候识别
     */
    private String cql;
    
    /**
     * CQL语句的头文摘要，用于在语法解析错误的时候显示
     */
    private String cqlSummary;
    
    /** <默认构造函数>
     *@param message 异常消息
     */
    public CQLException(String message)
    {
        super(message);
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     *@param cause 异常堆栈
     */
    public CQLException(String message, Throwable cause)
    {
        super(message, cause);
    }
    
    /**
     * <默认构造函数>
     *
     */
    public CQLException(ErrorCode errorCode, String... errorArgs)
    {
        super(errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     *
     */
    public CQLException(Throwable cause, ErrorCode errorCode, String... errorArgs)
    {
        super(cause, errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     * 仅供内部warp函数使用
     */
    protected CQLException(Throwable cause, String fullMessage, ErrorCode errorCode)
    {
        super(cause, fullMessage, errorCode);
    }
    
    /**
     * 包装StreamingException
     *
     */
    public static CQLException wrapStreamingException(StreamingException exception)
    {
        return new CQLException(exception.getCause(), exception.getMessage(), exception.getErrorCode());
    }
    
    public String getCqlSummary()
    {
        return cqlSummary;
    }
    
    public String getCql()
    {
        return cql;
    }
    
    /**
     * 设置CQL语句并生成摘要
     *
     */
    public void setCql(String cql)
    {
        this.cql = cql;
        this.cqlSummary = createCQLSummary(cql);
    }
    
    private String createCQLSummary(String cql)
    {
        if (Strings.isNullOrEmpty(cql))
        {
            return cql;
        }
        
        //长度小于摘要最小长度，返回完整信息
        if (cql.length() <= SUMMARY_LENGTH)
        {
            return cql;
        }
        
        return cql.substring(0, SUMMARY_LENGTH) + SUMMARY_POSTFIX;
    }
    
}
