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

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;

/**
 * <GroupView>
 * <分组视图，指定分组表达式，每个分组表达式对应不同的子视图>
 * 
 */
public class GroupView extends ViewImpl
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -15047835163263165L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupView.class);
    
    /**
     * 分组表达式
     */
    private final IExpression[] exprs;
    
    /**
     * 分组值对应子视图集合，不同的分组值对应不同的子视图，然后通过MergeView进行汇聚进行处理。
     */
    private final Map<Object, Object> subViewsPerKey = new HashMap<Object, Object>();
    
    /**
     * 分组值对应子数据集合。
     */
    private final HashMap<Object, Pair<Object, Object>> groupedEvents = new HashMap<Object, Pair<Object, Object>>();
    
    /**
     * <默认构造函数>
     *@param exprs 分组表达式
     */
    public GroupView(IExpression[] exprs)
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
                LOG.debug("Create SubViews For GroupID: {}", groupKey);
                subViews = makeSubViews(this, groupKey);
                subViewsPerKey.put(groupKey, subViews);
            }
            
            updateChildViews(subViews, new IEvent[] {theEvent}, null);
        }
        else
        {
            // Algorithm for dispatching multiple events
            if (newData != null)
            {
                for (IEvent newValue : newData)
                {
                    handleEvent(newValue, true);
                }
            }
            
            if (oldData != null)
            {
                for (IEvent oldValue : oldData)
                {
                    handleEvent(oldValue, false);
                }
            }
            
            // Update child views
            for (Map.Entry<Object, Pair<Object, Object>> entry : groupedEvents.entrySet())
            {
                IEvent[] newEvents = convertToArray(entry.getValue().getFirst());
                IEvent[] oldEvents = convertToArray(entry.getValue().getSecond());
                updateChildViews(entry.getKey(), newEvents, oldEvents);
            }
            
            groupedEvents.clear();
            
        }
    }
    
    private void updateChildViews(Object subViews, IEvent[] newData, IEvent[] oldData)
    {
        if (null != subViews)
        {
            if (subViews instanceof IView)
            {
                ((IView)subViews).update(newData, null);
            }
        }
        
    }
    
    /**
     * <处理一条事件>
     */
    private void handleEvent(IEvent theEvent, boolean isNew)
    {
        Object groupKey = getGroupKey(theEvent);
        
        // 根据GroupKey获取对应子View信息
        Object subViews = subViewsPerKey.get(groupKey);
        
        // 如果没有对应子View信息，则构建子View信息。
        if (subViews == null)
        {
            subViews = makeSubViews(this, groupKey);
            subViewsPerKey.put(groupKey, subViews);
        }
        
        //根据子View信息获取对应新旧数据，如果没有则构建新旧数据容器
        Pair<Object, Object> pair = groupedEvents.get(subViews);
        if (pair == null)
        {
            pair = new Pair<Object, Object>(null, null);
            groupedEvents.put(subViews, pair);
        }
        
        //根据是否新数据，加入容器制定位置
        if (isNew)
        {
            pair.setFirst(addToList(pair.getFirst(), theEvent));
        }
        else
        {
            pair.setSecond(addToList(pair.getSecond(), theEvent));
        }
    }
    
    /**
     * <返回新的容器， 如果容器为空，返回事件；否则判断当前容器是单一事件还是队列，如果是单一事件则构建队列，将原始事件和当前事件加入队列，否则将当前事件加入队列。>
     */
    @SuppressWarnings("unchecked")
    private Object addToList(Object container, IEvent theEvent)
    {
        if (container == null)
        {
            return theEvent;
        }
        else if (container instanceof Deque)
        {
            ArrayDeque<IEvent> deque = (ArrayDeque<IEvent>)container;
            deque.add(theEvent);
            return deque;
        }
        else
        {
            ArrayDeque<IEvent> deque = new ArrayDeque<IEvent>();
            deque.add((IEvent)container);
            deque.add(theEvent);
            return deque;
        }
    }
    
    @SuppressWarnings("unchecked")
    private IEvent[] convertToArray(Object eventOrDeque)
    {
        if (eventOrDeque == null)
        {
            return null;
        }
        if (eventOrDeque instanceof IEvent)
        {
            return new IEvent[] {(IEvent)eventOrDeque};
        }
        
        ArrayDeque<IEvent> deque = (ArrayDeque<IEvent>)eventOrDeque;
        
        return deque.toArray(new IEvent[deque.size()]);
    }
    
    /**
     * <对分组值创建子视图副本链>
     */
    private IView makeSubViews(GroupView groupView, Object groupKey)
    {
        if (!groupView.hasViews())
        {
            String message = "Unexpected empty list of child nodes for group view";
            LOG.error("Make sub views error: " + message);
            throw new RuntimeException(message);
        }
        
        if (groupView.getViews().size() > 1)
        {
            //TODO 暂不支持有多个子View
            String message = "Unexpected child nodes number for group view";
            LOG.error("Sub views number greater than 1.");
            throw new RuntimeException(message);
        }
        
        IView childView = groupView.getViews().get(0);
        IView renewView = copyChildView(groupView, childView);
        
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
    private IView copyChildView(GroupView groupView, IView childView)
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
}
