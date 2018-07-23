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

import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.view.IView;

/**
 * <自然天滑动窗口>
 * <自然天滑动窗口， 根据进入窗口事件时间判断窗口中事件是否属于同一自然天，如果不属于则移出窗口。>
 * 
 */
public class NaturalDaySlideWindow extends EventTimeBasedWindow
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -2937974298779456138L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(NaturalDaySlideWindow.class);
    
    /**
     * 自然天时间长度
     */
    private static final long NATUEALDAY = 24 * 3600 * 1000;
    
    /**
     * 窗口事件保存对象，记录时间与事件的索引方便得到过期事件
     */
    private TimeSlideEventList events = new TimeSlideEventList();
    
    /**
     * <默认构造函数>
     *@param timeExpr 时间属性表达式
     */
    public NaturalDaySlideWindow(IExpression timeExpr)
    {
        super(NATUEALDAY, timeExpr);
    }
    
    /** {@inheritDoc} */
    
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        long timestamp = -1;
        if (newData != null)
        {
            for (int i = 0; i < newData.length; i++)
            {
                timestamp = getTimestamp(newData[i]);
                events.add(timestamp, newData[i]);
            }
        }
        
        IEvent[] expired = null;
        if (timestamp != -1)
        {
            SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");
            ParsePosition pos = new ParsePosition(0);
            Date strtodate = formatter.parse(formatter.format(timestamp), pos);
            
            expired = events.getOldData(strtodate.getTime());
        }
        
        IEvent[] oldEvents = null;
        oldEvents = expired;
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            dataCollection.update(newData, oldEvents);
        }
        
        if (hasViews())
        {
            updateChild(newData, oldEvents);
        }
        
    }
    
    /** {@inheritDoc} */
    
    @Override
    public IView renewView()
    {
        NaturalDaySlideWindow renewWindow = new NaturalDaySlideWindow(getTimeExpr());
        
        IDataCollection dataCollection = getDataCollection();
        if (dataCollection != null)
        {
            IDataCollection renewCollection = dataCollection.renew();
            renewWindow.setDataCollection(renewCollection);
        }
        
        return renewWindow;
    }
    
}
