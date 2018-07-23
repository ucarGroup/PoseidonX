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

package com.huawei.streaming.view;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.processor.IProcessor;

/**
 * <特殊视图，使用逻辑处理器对新数据和过期数据进行逻辑处理。>
 * 
 */
public class ProcessView implements IView
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4259489275132557001L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ProcessView.class);
    
    /**
     * 父视图
     */
    private IViewable parent;
    
    /**
     * 处理器对象
     */
    private IProcessor processor = null;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        if (null == processor)
        {
            String msg = "Processor is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        processor.process(newData, oldData);
    }
    
    /**
     * <获取逻辑处理器>
     */
    public IProcessor getProcessor()
    {
        return processor;
    }
    
    /**
     * <设置逻辑处理器>
     */
    public void setProcessor(IProcessor processor)
    {
        this.processor = processor;
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public IView addView(IView view)
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public List<IView> getViews()
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public boolean removeView(IView view)
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public void removeAllViews()
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     * 
     */
    @Override
    public boolean hasViews()
    {
        throw new RuntimeException("not supported");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IViewable getParent()
    {
        return parent;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParent(IViewable parent)
    {
        this.parent = parent;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        
    }
}
