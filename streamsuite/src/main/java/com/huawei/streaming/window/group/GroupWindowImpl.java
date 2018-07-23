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

package com.huawei.streaming.window.group;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IRenew;
import com.huawei.streaming.view.IView;
import com.huawei.streaming.view.MergeView;
import com.huawei.streaming.view.ViewImpl;

/**
 * <分组窗口抽象类>
 * 
 */
public abstract class GroupWindowImpl extends ViewImpl implements IGroupWindow
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -7378858696426198680L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupWindowImpl.class);
    
    /**
     * 分组表达式
     */
    private final IExpression[] exprs;
    
    /**
     * 分组值对应子视图集合，不同的分组值对应不同的子视图，然后通过MergeView进行汇聚进行处理。
     */
    private final Map<Object, Object> subViewsPerKey = new HashMap<Object, Object>();
    
    /**
     * 窗口事件缓存集合
     */
    private IDataCollection dataCollection;
    
    /**
     * 分组值对应窗口事件缓存集合
     */
    private Map<Object, IDataCollection> subCollectionPerKey = new HashMap<Object, IDataCollection>();
    
    /**
     * <默认构造函数>
     *@param exprs 分组表达式
     */
    public GroupWindowImpl(IExpression[] exprs)
    {
        if (null == exprs)
        {
            String msg = "Invalid group expression";
            LOG.error(msg + ",expresison is null.");
            throw new IllegalArgumentException(msg);
        }
        
        if (exprs.length < 1)
        {
            String msg = "Invalid group expression";
            LOG.error(msg + ",expression size :{}.", exprs.length);
            throw new IllegalArgumentException(msg);
        }
        
        this.exprs = exprs;
    }
    
    /**
     * <对分组值创建子视图副本链>
     */
    protected IView makeSubViews(IGroupWindow groupWindow, Object groupKey)
    {
        if (!groupWindow.hasViews())
        {
            String message = "Unexpected empty list of child nodes for group view";
            LOG.error("Make sub views error: " + message);
            throw new RuntimeException(message);
        }
        
        if (groupWindow.getViews().size() > 1)
        {
            //TODO 暂不支持有多个子View
            String message = "Unexpected child nodes number for group view";
            LOG.error("Sub views number greater than 1.");
            throw new RuntimeException(message);
        }
        
        IView childView = groupWindow.getViews().get(0);
        
        IView renewView = copyChildView(groupWindow, childView);
        
        return renewView;
        
    }
    
    /**
     * <根据事件求分组值>
     */
    protected Object getGroupKey(IEvent theEvent)
    {
        if (exprs.length == 1)
        {
            return exprs[0].evaluate(theEvent);
        }
        
        Object[] values = new Object[exprs.length];
        for (int i = 0; i < exprs.length; i++)
        {
            values[i] = exprs[i].evaluate(theEvent);
        }
        return new MultiKey(values);
    }
    
    /**
     * <求视图副本链>
     */
    private IView copyChildView(IGroupWindow groupView, IView childView)
    {
        if (childView instanceof MergeView)
        {
            ((MergeView)childView).addParentView(groupView);
            return childView;
        }
        
        if (!(childView instanceof IRenew))
        {
            throw new RuntimeException("Unexpected error copying subview " + childView.getClass().getName());
        }
        
        IRenew renewChildView = (IRenew)childView;
        IView renewView = renewChildView.renewView();
        renewView.setParent(groupView);
        
        // Make the sub views for child copying from the original to the child
        copySubViews(childView, renewView);
        
        return renewView;
    }
    
    /**
     * <递归求的视图副本链>
     */
    private void copySubViews(IView originalView, IView copyView)
    {
        for (IView subView : originalView.getViews())
        {
            if (subView instanceof MergeView)
            {
                copyView.addView(subView);
                ((MergeView)subView).addParentView(copyView);
            }
            else
            {
                if (!(subView instanceof IRenew))
                {
                    throw new RuntimeException("Unexpected error copying subview");
                }
                IRenew cloneableView = (IRenew)subView;
                IView copiedChild = cloneableView.renewView();
                copyView.addView(copiedChild);
                
                copySubViews(subView, copiedChild);
            }
        }
    }
    
    /** {@inheritDoc} */
    
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        //TODO 仅考虑GroupWin为第一个窗口，且新数据只有一条
        
        if ((newData != null) && (oldData == null) && (newData.length == 1))
        {
            IEvent theEvent = newData[0];
            
            Object groupKey = getGroupKey(theEvent);
            
            Object subViews = subViewsPerKey.get(groupKey);
            
            // 当前为一个新的Group ID时，判断是否具有子视图，如果不存在则需要对该Group ID创建子视图
            
            if (null == subViews)
            {
                LOG.debug("Create SubViews For GroupID: {}.", groupKey);
                subViews = makeSubViews(this, groupKey);
                subViewsPerKey.put(groupKey, subViews);
            }
            
            IDataCollection subCollection = null;
            if (dataCollection != null)
            {
                subCollection = subCollectionPerKey.get(groupKey);
                
                if (null == subCollection)
                {
                    LOG.debug("Create subCollection For GroupID: {}.", groupKey);
                    subCollection = dataCollection.renew();
                    subCollectionPerKey.put(groupKey, subCollection);
                }
            }
            
            processGroupedEvent(subViews, subCollection, groupKey, theEvent);
        }
        else
        {
            String msg = "Current Only Support One New Event.";
            LOG.error(msg + "newData size :{}, oldData size :{}",
                newData == null ? 0 : newData.length,
                oldData == null ? 0 : oldData.length);
            
            throw new RuntimeException(msg);
            
        }
    }
    
    /**
     * <将事件加入分组对应的子视图链>
     */
    protected abstract void processGroupedEvent(Object subViews, IDataCollection subCollection, Object groupKey,
        IEvent theEvent);
    
    /**
     * <获取分组子视图对应关系>
     */
    protected final Map<Object, Object> getSubViewsPerKey()
    {
        return subViewsPerKey;
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
     * <获取分组子缓存集>
     */
    protected final Map<Object, IDataCollection> getSubCollectionPerKey()
    {
        return subCollectionPerKey;
    }
}
