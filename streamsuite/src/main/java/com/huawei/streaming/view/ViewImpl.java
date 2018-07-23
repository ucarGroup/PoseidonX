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

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;

/**
 * 
 * <视图抽象类>
 * 
 */
public abstract class ViewImpl implements IView
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 2003815073898329258L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ViewImpl.class);
    
    /**
     * 父视图
     */
    private IViewable parent;
    
    /**
     * 子视图集合
     */
    private final ArrayList<IView> subViews;
    
    /**
     * <默认构造函数>
     */
    public ViewImpl()
    {
        subViews = new ArrayList<IView>();
    }
    
    /**
     * {@inheritDoc}
     */
    public void removeAllViews()
    {
        subViews.clear();
    }
    
    /**
     * {@inheritDoc}
     */
    public List<IView> getViews()
    {
        return subViews;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean hasViews()
    {
        return !subViews.isEmpty();
    }
    
    /**
     * {@inheritDoc}
     */
    public IViewable getParent()
    {
        return parent;
    }
    
    /**
     * {@inheritDoc}
     */
    public void setParent(IViewable parent)
    {
        this.parent = parent;
    }
    
    /**
     * {@inheritDoc}
     */
    public IView addView(IView view)
    {
        if (null == view)
        {
            String msg = "View is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        subViews.add(view);
        view.setParent(this);
        return view;
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean removeView(IView view)
    {
        if (null == view)
        {
            String msg = "View is NULL.";
            LOG.error(msg);
            throw new RuntimeException(msg);
        }
        
        boolean isRemoved = subViews.remove(view);
        view.setParent(null);
        return isRemoved;
    }
    
    /**
     * <发送数据到子视图>
     */
    public void updateChild(IEvent[] newData, IEvent[] oldData)
    {
        for (IView child : subViews)
        {
            child.update(newData, oldData);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void start()
    {
        for (IView child : subViews)
        {
            child.start();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void stop()
    {
        for (IView child : subViews)
        {
            child.stop();
        }
    }
}
