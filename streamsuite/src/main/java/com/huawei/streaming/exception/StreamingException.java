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
 * 流处理的基础异常
 * 流处理系统中，所有的异常均应该继承自这个异常，然后再按照类型不同，进行细分
 * <p/>
 * 该异常属于编译时异常，在CQL提交任务的时候就可以捕获到；
 * 不同于StreamingRuntimeException,在编译期无法主动捕获。
 *
 */
public class StreamingException extends Exception
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -1650293190657737465L;
    
    /**
     * 异常码
     */
    private ErrorCode errorCode = null;
    
    /** <默认构造函数>
     *@param message 异常消息
     */
    public StreamingException(String message)
    {
        super(message);
        errorCode = ErrorCode.UNKNOWN_ERROR;
    }
    
    /** <默认构造函数>
     *@param message 异常消息
     *@param cause 异常堆栈
     */
    public StreamingException(String message, Throwable cause)
    {
        super(message, cause);
        errorCode = ErrorCode.UNKNOWN_ERROR;
    }
    
    /**
     * <默认构造函数>
     *
     */
    public StreamingException(ErrorCode code, String... errorArgs)
    {
        this(null, code, errorArgs);
    }
    
    /**
     * <默认构造函数>
     *
     */
    public StreamingException(Throwable cause, ErrorCode code, String... errorArgs)
    {
        super(code.getFullMessage(errorArgs), cause);
        errorCode = code;
    }
    
    /**
     * <默认构造函数>
     * 仅供内部warp函数使用
     *
     */
    protected StreamingException(Throwable cause, String fullMessage, ErrorCode code)
    {
        super(fullMessage, cause);
        this.errorCode = code;
    }
    
    /**
     * 包装StreamingException
     *
     */
    public static StreamingException wrapException(Exception exception)
    {
        return new StreamingException(exception.getCause(), ErrorCode.UNKNOWN_ERROR, exception.getMessage());
    }
    
    public ErrorCode getErrorCode()
    {
        return errorCode;
    }
}
