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

package com.huawei.streaming.window;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IRenew;
import com.huawei.streaming.view.ViewImpl;

/**
 * <长度窗口抽象类>
 * 
 */
public abstract class LengthBasedWindow extends ViewImpl implements IWindow, IRenew
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -591855531989393141L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(LengthBasedWindow.class);
    
    /**
     * 窗口事件保持长度
     */
    private int keepLength;
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * <默认构造函数>
     *@param keepLength 窗口保持长度
     */
    public LengthBasedWindow(int keepLength)
    {
        super();
        if (keepLength > 0)
        {
            this.keepLength = keepLength;
            LOG.debug("Length Window Keep Length: {}.", keepLength);
        }
        else
        {
            LOG.error("Invalid keepLength:  {}.", keepLength);
            throw new IllegalArgumentException("Invalid keepLength: " + keepLength);
        }
    }
    
    /**
     * <获取窗口保持长度 >
     */
    public int getKeepLength()
    {
        return keepLength;
    }
    
    /**
     * {@inheritDoc}
     */
    public void setDataCollection(IDataCollection dataCollection)
    {
        if (dataCollection == null)
        {
            LOG.error("Invalid dataCollection.");
            throw new IllegalArgumentException("Invalid dataCollection");
        }
        
        this.dataCollection = dataCollection;
    }
    
    /**
     * 返回窗口事件缓存集
     */
    public IDataCollection getDataCollection()
    {
        return dataCollection;
    }
}
