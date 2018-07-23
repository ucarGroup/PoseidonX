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
/*
 * 文 件 名:  FunctorView.java
 * 版 本 号:  V1.0.0
 * 版    权:  Huawei Technologies Co., Ltd. Copyright 1988-2008,  All rights reserved
 * 描    述:  <描述>
 * 作    者:  j00199894
 * 创建日期:  2013-7-23
 */
package com.huawei.streaming.view;

import com.huawei.streaming.event.IEvent;

/**
 * Functor功能视图
 * 是否需要判断左值和右值的合法性
 */
public class FunctorView extends ViewImpl
{
    
    private static final long serialVersionUID = -7149861251125803604L;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        updateChild(newData, oldData);
    }
    
}
