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

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;

/**
 * 
 * 过滤功能视图
 * <功能详细描述>
 * 
 */
public class FilterView extends ViewImpl implements IRenew
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -6936987505013321181L;
    
    private IExpression boolexpr;
    
    /**
     * <默认构造函数>
     *@param exp 过滤BOOLEAN表达式
     */
    public FilterView(IExpression exp)
    {
        this.boolexpr = exp;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        IEvent[] newE = filterEvent(newData, true);
        IEvent[] oldE = filterEvent(oldData, false);
        
        if (newE != null || oldE != null)
        {
            this.updateChild(newE, oldE);
        }
    }
    
    private IEvent[] filterEvent(IEvent[] events, boolean isNewData)
    {
        if (events == null)
        {
            return null;
        }
        
        List<IEvent> qualified = new ArrayList<IEvent>();
        Boolean pass = null;
        for (IEvent e : events)
        {
            pass = (Boolean)boolexpr.evaluate(e);
            if (null != pass && pass)
            {
                qualified.add(e);
            }
        }
        
        if (qualified.size() > 0)
        {
            return qualified.toArray(new IEvent[qualified.size()]);
        }
        else
        {
            return null;
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IView renewView()
    {
        return new FilterView(boolexpr);
    }
    
    public IExpression getBoolexpr()
    {
        return boolexpr;
    }
}
