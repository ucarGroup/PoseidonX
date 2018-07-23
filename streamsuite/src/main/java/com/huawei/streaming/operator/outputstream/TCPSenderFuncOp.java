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
package com.huawei.streaming.operator.outputstream;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.serde.BaseSerDe;
import com.huawei.streaming.serde.StreamSerDe;
import com.huawei.streaming.util.StreamingUtils;

/**
 * TCP bolt，通過socket發送數據
 */
public class TCPSenderFuncOp implements IOutputStreamOperator
{
    private static final Logger LOG = LoggerFactory.getLogger(TCPSenderFuncOp.class);
    
    //Automatically generate a serial version ID
    private static final long serialVersionUID = 5433931984789791657L;
    
    private transient Socket s;

    private String tcpServer;
    
    private Integer tcpPort;
    
    private Integer tcpConnectSessionTimeout;

    private StreamSerDe serde;

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
        this.config = conf;
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
    public void initialize() throws StreamingException
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        LOG.debug("In execute of TCPSenderFuncOp");
        try
        {
            this.s = new Socket();
            s.connect(new InetSocketAddress(tcpServer, tcpPort), tcpConnectSessionTimeout);
            Object obj = serde.serialize(BaseSerDe.changeEventsToList(event));
            if (obj == null)
            {
                LOG.warn("Ignore a null result in output.");
                return;
            }

            if (obj instanceof List)
            {
                for (Object value : (List)obj)
                {
                    s.getOutputStream().write((byte[])value);
                }
            }
            else
            {
                s.getOutputStream().write((byte[])obj);
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
        finally
        {
            StreamingUtils.close(s);
        }
        LOG.debug("TCPSenderOp sending end.");
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
