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

/**
 * 应用程序构建异常
 * 该异常主要发生在CQL语句经过语法解析之后，转化为API的各个算子期间
 *
 */
public class ApplicationBuildException extends ExecutorException
{
    
    private static final long serialVersionUID = 8758598200910253410L;
    
    /**
     * <默认构造函数>
     *
     */
    public ApplicationBuildException(ErrorCode errorCode, String... errorArgs)
    {
        super(errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     *
     */
    public ApplicationBuildException(Throwable cause, ErrorCode errorCode, String... errorArgs)
    {
        super(cause, errorCode, errorArgs);
    }
    
    /**
     * <默认构造函数>
     * 仅供内部warp函数使用
     *
     */
    protected ApplicationBuildException(Throwable cause, String fullMessage, ErrorCode errorCode)
    {
        super(cause, fullMessage, errorCode);
    }
    
}
