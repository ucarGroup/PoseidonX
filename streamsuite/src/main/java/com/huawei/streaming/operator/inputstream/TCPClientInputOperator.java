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
package com.huawei.streaming.operator.inputstream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;
import com.huawei.streaming.util.StreamingUtils;

/**
 * TCP Spout
 */
public class TCPClientInputOperator implements IInputStreamOperator
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPClientInputOperator.class);
    
    //Automatically generate a serial version ID
    private static final long serialVersionUID = 4927492973217406542L;

    private String tcpServer;
    
    private Integer tcpPort;
    
    private Integer tcpConnectSessionTimeout;

    private IEmitter emitter;

    private StreamSerDe serde;

    /**
     * 每个数据包的大小
     */
    private int binaryPackageSize = 883;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        this.tcpServer = conf.getStringValue(StreamingConfig.OPERATOR_TCPCLIENT_SERVER);
        this.tcpPort = conf.getIntValue(StreamingConfig.OPERATOR_TCPCLIENT_PORT);
        this.tcpConnectSessionTimeout = conf.getIntValue(StreamingConfig.OPERATOR_TCPCLIENT_SESSIONTIMEOUT);
        this.binaryPackageSize = conf.getIntValue(StreamingConfig.OPERATOR_TCPCLIENT_PACKAGELENGTH);
        config = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamingConfig getConfig()
    {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {

    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute()
        throws StreamingException
    {
        Socket s = new Socket();
        byte[] buffer = new byte[binaryPackageSize];
        try
        {
            s.connect(new InetSocketAddress(tcpServer, tcpPort), tcpConnectSessionTimeout);
            while((s.getInputStream().read(buffer)) != -1)
            {
                emitData(buffer);
            }
        }
        catch (IOException e)
        {
            LOG.warn("Ignore a IOException.", e);
        }
        catch (StreamSerDeException e)
        {
            LOG.warn("Ignore a serde exception.", e);
        }
        catch (Exception e)
        {
            //这里会有很多的runtime异常和其它异常，所以还是需要统一处理
            LOG.warn("Ignore an Exception.", e);
        }
        finally
        {
            StreamingUtils.close(s);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        
    }
    
    /**
     * 将得到的数据按行发送给bolt
     */
    private void emitData(Object obj)
        throws StreamSerDeException, StreamingException
    {
        //解析文件内容应放入function中来实现
        List<Object[]> vals = serde.deSerialize(obj);
        if(vals == null || vals.size() == 0)
        {
            return;
        }

        for (Object[] value : vals)
        {
            emitter.emit(value);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEmitter(IEmitter iEmitter)
    {
        this.emitter = iEmitter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe)
    {
        this.serde = streamSerDe;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }
}
