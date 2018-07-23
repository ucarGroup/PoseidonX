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

import com.huawei.streaming.event.IEvent;

/**
 * <特殊视图，MergeView可以使得GroupView按照GroupID分裂后的数据可以传递给同一个ProcessView。
 * MergeView与GroupView配合使用。>
 * 
 */
public final class MergeView extends ViewImpl
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 283190398830702409L;
    
    /**
     * 不同分组值的父视图集合
     */
    private final ArrayDeque<IView> parentViews = new ArrayDeque<IView>();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public final void update(IEvent[] newData, IEvent[] oldData)
    {
        updateChild(newData, oldData);
    }
    
    /**
     * <添加新分组值对应的父视图>
     */
    public final void addParentView(IView parentView)
    {
        parentViews.add(parentView);
    }
    
}
