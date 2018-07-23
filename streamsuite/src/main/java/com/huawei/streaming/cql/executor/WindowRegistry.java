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
package com.huawei.streaming.cql.executor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import com.huawei.streaming.api.opereators.WindowCommons;
import com.huawei.streaming.cql.executor.windowcreater.*;
import com.huawei.streaming.window.*;
import com.huawei.streaming.window.group.GroupEventTimeBatchWindow;
import com.huawei.streaming.window.group.GroupEventTimeSlideWindow;
import com.huawei.streaming.window.group.GroupLengthBatchWindow;
import com.huawei.streaming.window.group.GroupLengthSlideWindow;
import com.huawei.streaming.window.group.GroupNaturalDaySlideWindow;
import com.huawei.streaming.window.group.GroupTimeBatchWindow;
import com.huawei.streaming.window.group.GroupTimeSlideWindow;
import com.huawei.streaming.window.sort.LengthSortWindow;
import com.huawei.streaming.window.sort.TimeSortWindow;

/**
 * 系统window类函数的注册
 * 之所以将window的函数注册独立开来，
 * 是由于window的位置十分明显，容易辨别，
 * 不像UDAF，UDTF，UDF函数之类，难以区分。
 * 
 */
public class WindowRegistry extends WindowCommons
{
    /**
     * 窗口名称和窗口底层具体实现映射关系
     */
    private static final Map<String, WindowInfo> WINDOW_FUNCTIONS =
        Collections.synchronizedMap(new LinkedHashMap<String, WindowInfo>());
    
    static
    {
        /**
         * 当前系统内没有now的窗口
         * select * from S 就没有窗口
         * 
         * 另外，只要CQL语句中没有显示的定义窗口，那么就没有窗口
         **/
        registerNativeSlideWindow(new WindowInfo(KEEPALL_WINDOW, KeepAllWindow.class, KeepAllWindowCreator.class, ""));
        
        registerNativeSlideWindow(new WindowInfo(TIME_SLIDE_WINDOW, TimeSlideWindow.class,
            TimeSlideWindowCreator.class, "length,[excludeNow]"));
        registerNativeBatchWindow(new WindowInfo(TIME_BATCH_WINDOW, TimeBatchWindow.class,
            TimeBatchWindowCreator.class, "length,[excludeNow]"));
        registerNativeSlideWindow(new WindowInfo(LENGTH_SLIDE_WINDOW, LengthSlideWindow.class,
            LengthSlideWindowCreator.class, "length,[excludeNow]"));
        registerNativeBatchWindow(new WindowInfo(LENGTH_BATCH_WINDOW, LengthBatchWindow.class,
            LengthBatchWindowCreator.class, "length,[excludeNow]"));

        registerNativeBatchWindow(new WindowInfo(TIME_ACCUMBATCH_WINDOW, TimeAccumBatchWindow.class,
                TimeAccumBatchWindowCreator.class, "length,[excludeNow]"));

        
        registerNativeSlideWindow(new WindowInfo(GROUP_TIME_SLIDE_WINDOW, GroupTimeSlideWindow.class,
            GroupTimeSlideWindowCreator.class, "length,groupbyExpression,[excludeNow]"));
        registerNativeBatchWindow(new WindowInfo(GROUP_TIME_BATCH_WINDOW, GroupTimeBatchWindow.class,
            GroupTimeBatchWindowCreator.class, "length,groupbyExpression,[excludeNow]"));
        registerNativeSlideWindow(new WindowInfo(GROUP_LENGTH_SLIDE_WINDOW, GroupLengthSlideWindow.class,
            GroupLengthSlideWindowCreator.class, "length,groupbyExpression,[excludeNow]"));
        registerNativeBatchWindow(new WindowInfo(GROUP_LENGTH_BATCH_WINDOW, GroupLengthBatchWindow.class,
            GroupLengthBatchWindowCreator.class, "length,groupbyExpression,[excludeNow]"));
        
        registerNativeSlideWindow(new WindowInfo(LENGTH_SORT_WINDOW, LengthSortWindow.class,
            LengthSortWindowCreator.class, "length,orderbyExpression,[excludeNow]"));
        registerNativeSlideWindow(new WindowInfo(TIME_SORT_WINDOW, TimeSortWindow.class, TimeSortWindowCreator.class,
            "length,orderbyExpression,[excludeNow]"));
        
        /*
         * 事件驱动的窗口
         */
        registerNativeBatchWindow(new WindowInfo(EVENT_TBATCH_WINDOW, EventTimeBatchWindow.class,
            EventTimeBatchWindowCreator.class, "length,timestampField,[excludeNow]"));
        registerNativeSlideWindow(new WindowInfo(EVENT_TSLIDE_WINDOW, EventTimeSlideWindow.class,
            EventTimeSlideWindowCreator.class, "length,timestampField,[excludeNow]"));
        registerNativeBatchWindow(new WindowInfo(GROUP_EVENT_TBATCH_WINDOW, GroupEventTimeBatchWindow.class,
            GroupEventTimeBatchWindowCreator.class, "length,groupbyExpression,timestampField,[excludeNow]"));
        registerNativeSlideWindow(new WindowInfo(GROUP_EVENT_TSLIDE_WINDOW, GroupEventTimeSlideWindow.class,
            GroupEventTimeSlideWindowCreator.class, "length,groupbyExpression,timestampField,[excludeNow]"));
        
        /*
         *  自然天的窗口
         *  窗口中仅保存当天数据，超过当天即过期
         */
        registerNativeSlideWindow(new WindowInfo(TODAY_WINDOW, NaturalDaySlideWindow.class,
            NaturalDaySlideWindowCreator.class, "timestampField,[excludeNow]"));
        registerNativeSlideWindow(new WindowInfo(GROUP_TODAY_WINDOW, GroupNaturalDaySlideWindow.class,
            GroupNaturalDaySlideWindowCreator.class, "groupbyExpression,timestampField,[excludeNow]"));
    }
    
    /**
     * 注册窗口
     */
    public static void registerWindow(WindowInfo winInfo)
    {
        winInfo.setNative(false);
        winInfo.setIsSlide(null);
        WINDOW_FUNCTIONS.put(winInfo.getWidowName(), winInfo);
    }
    
    /**
     * 注册窗口
     */
    private static void registerNativeSlideWindow(WindowInfo winInfo)
    {
        winInfo.setNative(true);
        winInfo.setIsSlide(true);
        WINDOW_FUNCTIONS.put(winInfo.getWidowName(), winInfo);
    }
    
    /**
     * 注册窗口
     */
    private static void registerNativeBatchWindow(WindowInfo winInfo)
    {
        winInfo.setNative(true);
        winInfo.setIsSlide(false);
        WINDOW_FUNCTIONS.put(winInfo.getWidowName(), winInfo);
    }
    
    /**
     * 获取窗口信息
     */
    public static WindowInfo getWindowInfo(String alias)
    {
        return WINDOW_FUNCTIONS.get(alias);
    }
    
    /**
     * 根据window的别名，获取窗口实例类
     * 
     */
    public static Class< ? extends WindowCreator> getWindowCreatorByAlias(String alias)
    {
        return WINDOW_FUNCTIONS.get(alias).getCreatorClass();
    }
    
    /**
     * 根据窗口函数短名称获取该窗口所在类
     * 
     */
    public static Class< ? extends IWindow> getWindowClassByName(String windowName)
    {
        return WINDOW_FUNCTIONS.get(windowName).getInstanceClass();
    }
    
    /**
     * 根据窗口类获取窗口所在函数短名称
     * 主要在IDE这边用到
     * 
     */
    public static String getWindowNameByClass(Class< ? extends IWindow> clazz)
    {
        for (Entry<String, WindowInfo> et : WINDOW_FUNCTIONS.entrySet())
        {
            if (clazz == et.getValue().getInstanceClass())
            {
                return et.getKey();
            }
        }
        return null;
    }
    
    /**
     * 根据窗口类的全名称或者窗口函数短名称
     * 主要是IDE在用
     * 
     */
    public static String getWindowNameByClass(String clazz)
    {
        for (Entry<String, WindowInfo> et : WINDOW_FUNCTIONS.entrySet())
        {
            if (et.getValue().getInstanceClass().toString().equals(clazz))
            {
                return et.getKey();
            }
        }
        return null;
    }
}
