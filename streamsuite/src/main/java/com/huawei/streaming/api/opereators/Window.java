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

package com.huawei.streaming.api.opereators;

import static com.huawei.streaming.api.opereators.WindowCommons.EVENT_TBATCH_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.EVENT_TSLIDE_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.GROUP_EVENT_TBATCH_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.GROUP_EVENT_TSLIDE_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.GROUP_LENGTH_BATCH_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.GROUP_LENGTH_SLIDE_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.GROUP_TIME_BATCH_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.GROUP_TIME_SLIDE_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.KEEPALL_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.LENGTH_BATCH_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.LENGTH_SLIDE_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.LENGTH_SORT_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.TIME_BATCH_WINDOW;
import static com.huawei.streaming.api.opereators.WindowCommons.TIME_SLIDE_WINDOW;

import java.beans.Transient;
import java.util.List;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * 窗口
 * 
 * 2013.07.26讨论结果
 * join窗口中不包含filter表达式
 * 
 */
@XStreamAlias("Window")
public class Window
{
    /**
     * 窗口名称
     */
    @XStreamAsAttribute
    private String name;
    
    /**
     * 窗口长度或者时间长度
     */
    private Long length;
    
    /**
     * groupby表达式
     */
    private String groupbyExpression;
    
    /**
     * orderby表达式
     */
    private String orderbyExpression;
    
    /**
     * 用户自定义窗口参数
     */
    private List<String> udfWindowArgs;
    
    /**
     * 时间戳字段
     */
    private String timestampField;
    
    /**
     * 是否排除当前事件
     */
    private Boolean excludeNow = false;

    private String outputExpression;
    
    /**
     * <默认构造函数>
     * 这里把构造函数写为私有的，防止ClientAPI用户使用。
     */
    private Window(String name)
    {
        super();
        this.name = name;
    }
    
    /**
     * 创建KeepAllwindow
     * 
     * 该window，没有过期数据，每进来一个事件，就产生一个istream事件。
     * 所有的事件都在窗口中保存着，
     * 所以在使用的时候要注意，防止数据保存太多，而导致内存溢出。
     * 这个窗口主要用来保存一些不怎么改变的，比较小的数据，比如类型信息之类。
     * 
     */
    public static Window createKeepAllWindow()
    {
        return new Window(KEEPALL_WINDOW);
    }
    
    /**
     * 创建时间滑动窗
     * 
     */
    public static Window createTimeSlideWindow(long keepTime)
    {
        Window w = new Window(TIME_SLIDE_WINDOW);
        w.setLength(keepTime);
        return w;
    }
    
    /**
     * 创建时间跳动窗口
     * 数据分批次过期。
     * 
     */
    public static Window createTimeBatchWindow(long keepTime)
    {
        Window w = new Window(TIME_BATCH_WINDOW);
        w.setLength(keepTime);
        return w;
    }
    
    /**
     * 创建长度滑动窗
     * 
     */
    public static Window createLengthSlideWindow(long keepLength)
    {
        Window w = new Window(LENGTH_SLIDE_WINDOW);
        w.setLength(keepLength);
        return w;
    }
    
    /**
     * 创建长度跳动窗
     * 
     */
    public static Window createLengthBatchWindow(long keepLength)
    {
        Window w = new Window(LENGTH_BATCH_WINDOW);
        w.setLength(keepLength);
        return w;
    }
    
    /**
     * 创建分组时间滑动窗
     * 
     */
    public static Window createGroupTimeSlideWindow(long keepTime, String groupbyExpression)
    {
        Window w = new Window(GROUP_TIME_SLIDE_WINDOW);
        w.setLength(keepTime);
        w.setGroupbyExpression(groupbyExpression);
        return w;
    }
    
    /**
     * 创建分组时间跳动窗
     */
    public static Window createGroupTimeBatchWindow(long keepTime, String groupbyExpression)
    {
        Window w = new Window(GROUP_TIME_BATCH_WINDOW);
        w.setLength(keepTime);
        w.setGroupbyExpression(groupbyExpression);
        return w;
    }
    
    /**
     * 创建分组长度滑动窗
     */
    public static Window createGroupLengthSlideWindow(long keepLength, String groupbyExpression)
    {
        Window w = new Window(GROUP_LENGTH_SLIDE_WINDOW);
        w.setLength(keepLength);
        w.setGroupbyExpression(groupbyExpression);
        return w;
    }
    
    /**
     * 创建分组长度跳动窗
     */
    public static Window createGroupLengthBatchWindow(long keepLength, String groupbyExpression)
    {
        Window w = new Window(GROUP_LENGTH_BATCH_WINDOW);
        w.setLength(keepLength);
        w.setGroupbyExpression(groupbyExpression);
        return w;
    }
    
    /**
     * 创建时间排序窗
     * 
     * 排序的时候按照时间顺序进行排序，所以只能按照一个字段进行排序，并且不能指定升序还是降序
     * 
     */
    public static Window createTimeSortWindow(long keepTime, String sortbyExpression)
    {
        Window w = new Window(EVENT_TBATCH_WINDOW);
        w.setLength(keepTime);
        w.setOrderbyExpression(sortbyExpression);
        return w;
    }
    
    /**
     * 创建长度排序窗口
     * 
     */
    public static Window createLengthSortWindow(long keepLength, String sortbyExpression)
    {
        Window w = new Window(LENGTH_SORT_WINDOW);
        w.setLength(keepLength);
        w.setOrderbyExpression(sortbyExpression);
        return w;
    }
    
    /**
     * 事件驱动的时间跳动窗
     */
    public static Window createEventTimeBatchWindow(long keeptime, String timestampField)
    {
        Window w = new Window(EVENT_TBATCH_WINDOW);
        w.setLength(keeptime);
        w.setTimestampField(timestampField);
        return w;
    }
    
    /**
     * 事件驱动的时间滑动窗
     */
    public static Window createEventTimeSlideWindow(long keeptime, String timestampField)
    {
        Window w = new Window(EVENT_TSLIDE_WINDOW);
        w.setLength(keeptime);
        w.setTimestampField(timestampField);
        return w;
    }
    
    /**
     * 事件驱动的分组时间跳动窗
     */
    public static Window createGroupEventTimeBatchWindow(long keeptime, String groupbyExpression, String timestampField)
    {
        Window w = new Window(GROUP_EVENT_TBATCH_WINDOW);
        w.setLength(keeptime);
        w.setTimestampField(timestampField);
        w.setGroupbyExpression(groupbyExpression);
        return w;
    }
    
    /**
     * 事件驱动的分组时间滑动窗
     */
    public static Window createGroupEventTimeSlideWindow(long keeptime, String groupbyExpression, String timestampField)
    {
        Window w = new Window(GROUP_EVENT_TSLIDE_WINDOW);
        w.setLength(keeptime);
        w.setTimestampField(timestampField);
        w.setGroupbyExpression(groupbyExpression);
        return w;
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public Long getLength()
    {
        return length;
    }
    
    public void setLength(Long length)
    {
        this.length = length;
    }
    
    public String getGroupbyExpression()
    {
        return groupbyExpression;
    }
    
    public void setGroupbyExpression(String groupbyExpression)
    {
        this.groupbyExpression = groupbyExpression;
    }
    
    public String getOrderbyExpression()
    {
        return orderbyExpression;
    }
    
    public void setOrderbyExpression(String orderbyExpression)
    {
        this.orderbyExpression = orderbyExpression;
    }
    
    public List<String> getUdfWindowArgs()
    {
        return udfWindowArgs;
    }
    
    public void setUdfWindowArgs(List<String> udfWindowArgs)
    {
        this.udfWindowArgs = udfWindowArgs;
    }
    
    public String getTimestampField()
    {
        return timestampField;
    }
    
    public void setTimestampField(String timestampField)
    {
        this.timestampField = timestampField;
    }
    
    public Boolean isExcludeNow()
    {
        return excludeNow;
    }
    
    public void setExcludeNow(Boolean excludeNow)
    {
        this.excludeNow = excludeNow;
    }

    /**
     * Getter for property 'outputExpression'.
     *
     * @return Value for property 'outputExpression'.
     */
    public String getOutputExpression() {
        return outputExpression;
    }

    /**
     * Setter for property 'outputExpression'.
     *
     * @param outputExpression Value to set for property 'outputExpression'.
     */
    public void setOutputExpression(String outputExpression) {
        this.outputExpression = outputExpression;
    }
}
