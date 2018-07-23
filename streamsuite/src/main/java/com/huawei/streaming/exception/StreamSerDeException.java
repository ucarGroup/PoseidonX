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
 * 数据序列化和反序列化的时候可能产生的异常
 * 
 */
public class StreamSerDeException extends Exception
{
    
    private static final long serialVersionUID = 4991711525715568679L;
    
    /**
     * <默认构造函数>
     */
    public StreamSerDeException()
    {
        super();
    }
    
    /**
     * <默认构造函数>
     */
    public StreamSerDeException(String message)
    {
        super(message);
    }
    
    /**
     * <默认构造函数>
     */
    public StreamSerDeException(Throwable cause)
    {
        super(cause);
    }
    
    /**
     * <默认构造函数>
     */
    public StreamSerDeException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
