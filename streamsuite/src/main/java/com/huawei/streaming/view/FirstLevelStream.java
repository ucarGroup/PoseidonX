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

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;

/**
 * 
 * <FirstLevelStream 实现第一层视图处理>
 * 
 */
public final class FirstLevelStream implements IViewable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -4993118574495471408L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(FirstLevelStream.class);
    
    /**
     * 子视图集合
     */
    private final LinkedList<IView> children = new LinkedList<IView>();
    
    /**
     * 最后加入事件
     */
    private IEvent lastInsertedEvent;
    
    /**
     * <接受外部数据>
     */
    public final void add(IEvent theEvent)
    {
        IEvent[] newData = new IEvent[] {theEvent};
        for (IView childView : children)
        {
            childView.update(newData, null);
        }
        
        lastInsertedEvent = theEvent;
    }
    
    /**
     * {@inheritDoc}
     */
    public final IView addView(IView view)
    {
        if (null == view)
        {
            String msg = "View is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        children.add(view);
        view.setParent(this);
        return view;
    }
    
    /**
     * {@inheritDoc}
     */
    public final List<IView> getViews()
    {
        return children;
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean removeView(IView view)
    {
        if (null == view)
        {
            String msg = "View is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        boolean isRemoved = children.remove(view);
        view.setParent(null);
        return isRemoved;
    }
    
    /**
     * {@inheritDoc}
     */
    public final boolean hasViews()
    {
        return !children.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    public void removeAllViews()
    {
        children.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        for (IView childView : children)
        {
            childView.start();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        for (IView childView : children)
        {
            childView.stop();
        }
    }
    
    /**
     * 返回 lastInsertedEvent
     */
    public final IEvent getLastInsertedEvent()
    {
        return lastInsertedEvent;
    }
    
}
