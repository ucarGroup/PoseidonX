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

import java.util.LinkedHashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * <特殊视图，调用Join处理器完成Join操作>
 * 
 */
public class JoinProcessView extends ProcessView
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 3503873571996480982L;
    
    private static final Logger LOG = LoggerFactory.getLogger(JoinProcessView.class);
    
    private Set<IView> parents;
    
    /**
     * <默认构造函数>
     *
     */
    public JoinProcessView()
    {
        LOG.debug("Initiate JoinProcessView.");
        parents = new LinkedHashSet<IView>();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public IViewable getParent()
    {
        throw new RuntimeException("getParent() is not supported in JoinProcessView");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParent(IViewable parent)
    {
        LOG.debug("set {} as parent view to JoinProcessView.", parent);
        parents.add((IView)parent);
    }
    
    /**
     * 获得所有父视图
     * <功能详细描述>
     */
    public IViewable[] getParents()
    {
        return parents.toArray(new IView[parents.size()]);
    }
}
