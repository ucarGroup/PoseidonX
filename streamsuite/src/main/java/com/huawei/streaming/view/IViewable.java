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

import java.io.Serializable;
import java.util.List;

/**
 * 
 * <IViewable接口标记视图支持一个或者多个子视图>
 * 
 */
public interface IViewable extends Serializable
{
    /**
     * <增加子视图>
     */
    public IView addView(IView view);
    
    /**
     * <获取子视图>
     */
    public List<IView> getViews();
    
    /**
     * <删除子视图>
     */
    public boolean removeView(IView view);
    
    /**
     * <删除全部子视图>
     */
    public void removeAllViews();
    
    /**
     * <是否包含子视图>
     */
    public boolean hasViews();
    
    /**
     * <启动>
     */
    public void start();
    
    /**
     * <停止>
     */
    public void stop();
}
