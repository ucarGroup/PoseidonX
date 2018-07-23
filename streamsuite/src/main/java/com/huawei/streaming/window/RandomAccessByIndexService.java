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
 * <通过索引获取窗口数据服务>
 * <通过索引获取窗口数据服务， 服务中保存窗口数据获取对象，通过该对象可以获取特定窗口缓存集中指定索引的事件。>
 * 
 */
public class RandomAccessByIndexService extends AbstractAccessService
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = 1892492968111771280L;
    
    /**
     * 窗口数据获取对象
     */
    private IRandomAccessByIndex randomAccessByIndex;
    
    /**
     * <默认构造函数>
     *
     */
    public RandomAccessByIndexService()
    {
        
    }
    
    /**
     * <得到窗口数据获取对象>
     * <得到窗口数据获取对象>
     */
    public IRandomAccessByIndex getAccessor()
    {
        return randomAccessByIndex;
    }
    
    /**
     * <更新当前Service中对应的窗口数据获取对象>
     * <更新当前Service中对应的窗口数据获取对象>
     */
    public void updated(IRandomAccessByIndex accssByIndex)
    {
        this.randomAccessByIndex = accssByIndex;
    }
}
