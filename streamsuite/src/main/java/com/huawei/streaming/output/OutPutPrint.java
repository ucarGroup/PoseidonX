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
 * 文 件 名:  OutPutPrint.java
 * 版 本 号:  V1.0.0
 * 版    权:  Huawei Technologies Co., Ltd. Copyright 1988-2008,  All rights reserved
 * 描    述:  <描述>
 * 作    者:  z00221388
 * 创建日期:  2013-6-13
 */
package com.huawei.streaming.output;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.TupleEventType;

/**
 * 输出测试类，打印到屏幕
 */
public class OutPutPrint implements IOutput
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 8512176298059174174L;
    
    /**
     * 执行输出操作
     */
    @SuppressWarnings("unchecked")
    @Override
    public void output(Object object)
    {
        if (null == object)
        {
            return;
        }
        
        Pair<IEvent[], IEvent[]> list = (Pair<IEvent[], IEvent[]>)object;
        
        if (null == list.getFirst())
        {
            return;
        }
        
        for (int i = 0; i < list.getFirst().length; i++)
        {
            
            IEvent tupleEvent = list.getFirst()[i];
            
            TupleEventType type = (TupleEventType)tupleEvent.getEventType();
            Attribute[] att = type.getAllAttributes();
            for (int j = 0; j < att.length; j++)
            {
                System.out.print(att[j].getAttName() + "=" + tupleEvent.getValue(att[j].getAttName()) + "    ");
            }
            System.out.println();
        }
    }
    
}
