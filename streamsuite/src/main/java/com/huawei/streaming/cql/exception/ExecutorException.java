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


import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * 执行期异常
 *
 */
public class ExecutorException extends CQLException
{
    private static final long serialVersionUID = 1596610837344537253L;
    
    /**
     * <默认构造函数>
     *
     */
    public ExecutorException(ErrorCode errorCode, String... errorArgs)
    {
        super(errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     *
     */
    public ExecutorException(Throwable cause, ErrorCode errorCode, String... errorArgs)
    {
        super(cause, errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     * 仅供内部warp函数使用
     *
     */
    protected ExecutorException(Throwable cause, String fullMessage, ErrorCode errorCode)
    {
        super(cause, fullMessage, errorCode);
    }
    
    /**
     * 包装系统异常到ExecutorException
     *
     */
    public static ExecutorException wrapException(Exception exception)
    {
        return new ExecutorException(exception.getCause(), ErrorCode.UNKNOWN_ERROR, exception.getMessage());
    }
    
    /**
     * 包装StreamingException
     *
     */
    public static ExecutorException wrapStreamingException(StreamingException exception)
    {
        return new ExecutorException(exception.getCause(), exception.getMessage(), exception.getErrorCode());
    }

    /**
     * 包装StreamingException
     *
     */
    public static ExecutorException wrapStreamingRunTimeException(StreamingRuntimeException exception)
    {
        if(exception.getErrorCode() != null)
        {
            return new ExecutorException(exception.getCause(), exception.getMessage(), exception.getErrorCode());
        }

        return new ExecutorException(exception.getCause(), exception.getMessage(), ErrorCode.UNKNOWN_ERROR);
    }
}
