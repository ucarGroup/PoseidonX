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
package com.huawei.streaming.exception;


/**
 * Streaming运行时异常
 * 所有的运行时异常，都继承自该类，或者直接抛出该类
 * 
 */
public class StreamingRuntimeException extends RuntimeException
{
    
    /**
     * 序列化
     */
    private static final long serialVersionUID = 1120103984629512199L;

    private ErrorCode errorCode = null;


    /** <默认构造函数>
     *
     */
    public StreamingRuntimeException()
    {
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     */
    public StreamingRuntimeException(String message)
    {
        super(message);
    }
    
    /** <默认构造函数>
     *@param cause 异常堆栈
     */
    public StreamingRuntimeException(Throwable cause)
    {
        super(cause);
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     *@param cause 异常堆栈
     */
    public StreamingRuntimeException(String message, Throwable cause)
    {
        super(message, cause);
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     *@param cause 异常堆栈
     *@param enableSuppression whether or not suppression is enabled
     *                          or disabled
     *@param writableStackTrace whether or not the stack trace should
     *                           be writable
     */
    public StreamingRuntimeException(String message, Throwable cause, boolean enableSuppression,
        boolean writableStackTrace)
    {
        super(message, cause, enableSuppression, writableStackTrace);
    }


    /**
     * <默认构造函数>
     *
     */
    public StreamingRuntimeException(ErrorCode code, String... errorArgs)
    {
        this(null, code, errorArgs);
    }

    /**
     * <默认构造函数>
     *
     */
    public StreamingRuntimeException(Throwable cause, ErrorCode code, String... errorArgs)
    {
        super(code.getFullMessage(errorArgs), cause);
        errorCode = code;
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

}
