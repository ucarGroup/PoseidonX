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

/**
 * <通过事件和相对索引获取窗口数据服务>
 * <通过事件和相对索引获取窗口数据服务>
 * 
 */
public class RelativeAccessByEventAndIndexService extends AbstractAccessService
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 5880829255166530065L;
    
    /**
     * 窗口数据获取对象
     */
    private IRelativeAccessByEventAndIndex relativeAccess;
    
    /**
     * <默认构造函数>
     *
     */
    public RelativeAccessByEventAndIndexService()
    {
        
    }
    
    /**
     * <得到窗口数据获取对象>
     * <得到窗口数据获取对象>
     */
    public IRelativeAccessByEventAndIndex getAccessor()
    {
        return relativeAccess;
    }
    
    /**
     * <更新当前Service中对应的窗口数据获取对象>
     * <更新当前Service中对应的窗口数据获取对象>
     */
    public void updated(IRelativeAccessByEventAndIndex accessByEventAndIndex)
    {
        this.relativeAccess = accessByEventAndIndex;
    }
    
}
