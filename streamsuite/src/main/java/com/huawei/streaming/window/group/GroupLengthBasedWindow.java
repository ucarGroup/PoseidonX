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

package com.huawei.streaming.window.group;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.expression.IExpression;

/**
 * <基于长度的分组窗口抽象类>
 * 
 */
public abstract class GroupLengthBasedWindow extends GroupWindowImpl
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 3216031941749505390L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(GroupLengthBasedWindow.class);
    
    /**
     * 窗口事件保持长度
     */
    private int keepLength;
    
    /**
     * <默认构造函数>
     */
    public GroupLengthBasedWindow(IExpression[] exprs, int keepLength)
    {
        super(exprs);
        
        if (keepLength > 0)
        {
            this.keepLength = keepLength;
        }
        else
        {
            LOG.error("Invalid keepLength: {}.", keepLength);
            throw new IllegalArgumentException("Invalid keepLength: " + keepLength);
        }
    }
    
    /**
     * <获取窗口保持长度 >
     */
    public long getKeepLength()
    {
        return keepLength;
    }
    
}
