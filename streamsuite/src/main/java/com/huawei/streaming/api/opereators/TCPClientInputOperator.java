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

package com.huawei.streaming.api.opereators;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.config.StreamingConfig;

/**
 * TCP数据读取算子
 *
 */
public class TCPClientInputOperator extends InnerInputSourceOperator
{
    /**
     * 地址
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_TCPCLIENT_SERVER)
    private String server;
    
    /**
     * 端口
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_TCPCLIENT_PORT)
    private String port;
    
    /**
     * 端口
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_TCPCLIENT_SESSIONTIMEOUT)
    private String sessionTimeout;

    /**
     * 每个包长度
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_TCPCLIENT_PACKAGELENGTH)
    private String packageLength;

    /**
     * <默认构造函数>
     *
     */
    public TCPClientInputOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getSessionTimeout()
    {
        return sessionTimeout;
    }
    
    public void setSessionTimeout(String sessionTimeout)
    {
        this.sessionTimeout = sessionTimeout;
    }
    
    public String getServer()
    {
        return server;
    }
    
    public void setServer(String server)
    {
        this.server = server;
    }
    
    public String getPort()
    {
        return port;
    }
    
    public void setPort(String port)
    {
        this.port = port;
    }

    public String getPackageLength()
    {
        return packageLength;
    }

    public void setPackageLength(String packageLength)
    {
        this.packageLength = packageLength;
    }

}
