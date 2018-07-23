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

package com.huawei.streaming.process;

import java.io.Serializable;
import java.util.Arrays;

import com.huawei.streaming.event.IEvent;

/**
 * limit操作类，返回指定条数的记录
 */
public class LimitProcess implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -7801685004807829972L;
    
    /**
     * 需要的记录条数
     */
    private int limitNum;
    
    /**
     * 
     * <默认构造函数>
     */
    public LimitProcess(int limitNum)
    {
        super();
        if (limitNum <= 0)
        {
            throw new IllegalArgumentException("The limitNum is invalid,limitNum=" + limitNum);
        }
        this.limitNum = limitNum;
    }
    
    /**
     * 执行limit操作
     */
    public IEvent[] process(IEvent[] theEvents)
    {
        if (null == theEvents || theEvents.length == 0 || theEvents.length <= limitNum)
        {
            return theEvents;
        }
        else
        {
            return Arrays.copyOfRange(theEvents, 0, limitNum);
        }
    }
}
