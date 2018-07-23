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

/**
 * 
 * 统一窗口名称
 * 
 */
public class WindowCommons
{
    /**
     * keepall 窗口
     */
    public static final String KEEPALL_WINDOW = "keepall";
    
    /**
     * 时间滑动窗
     */
    public static final String TIME_SLIDE_WINDOW = "time_slide";
    
    /**
     * 时间跳动窗
     */
    public static final String TIME_BATCH_WINDOW = "time_batch";

    /**
     * 累计时间时间跳动窗
     */
    public static final String TIME_ACCUMBATCH_WINDOW = "time_accumbatch";
    
    /**
     * 长度滑动窗
     */
    public static final String LENGTH_SLIDE_WINDOW = "length_slide";
    
    /**
     * 长度跳动窗
     */
    public static final String LENGTH_BATCH_WINDOW = "length_batch";
    
    /**
     * 分组时间滑动窗
     */
    public static final String GROUP_TIME_SLIDE_WINDOW = "group_time_slide";
    
    /**
     * 分组时间跳动窗
     */
    public static final String GROUP_TIME_BATCH_WINDOW = "group_time_batch";
    
    /**
     * 分组长度滑动窗
     */
    public static final String GROUP_LENGTH_SLIDE_WINDOW = "group_length_slide";
    
    /**
     * 分组长度跳动窗
     */
    public static final String GROUP_LENGTH_BATCH_WINDOW = "group_length_batch";
    
    /**
     * 长度排序窗口
     */
    public static final String LENGTH_SORT_WINDOW = "length_sort";
    
    /**
     * 时间排序窗口
     */
    public static final String TIME_SORT_WINDOW = "time_sort";
    
    /**
     * 事件驱动的事件跳动窗
     */
    public static final String EVENT_TBATCH_WINDOW = "event_tbatch";
    
    /**
     * 事件驱动的事件滑动窗
     */
    public static final String EVENT_TSLIDE_WINDOW = "event_tslide";
    
    /**
     * 事件驱动的分组事件跳动窗
     */
    public static final String GROUP_EVENT_TBATCH_WINDOW = "group_event_tbatch";
    
    /**
     * 事件驱动的分组事件滑动窗
     */
    public static final String GROUP_EVENT_TSLIDE_WINDOW = "group_event_tslide";
    
    /**
     * 自然天窗口
     */
    public static final String TODAY_WINDOW = "today";
    
    /**
     * 分组的自然天滑动窗
     */
    public static final String GROUP_TODAY_WINDOW = "group_today";
    
}
