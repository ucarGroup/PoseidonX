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

package com.huawei.streaming.expression;

import java.io.Serializable;

import com.huawei.streaming.event.IEvent;

/**
 * 
 * <表达式求值接口，对事件进行求值，返回结果>
 * 
 */
public interface IExpression extends Serializable
{
    /**
     * <单流事件表达式求值>
     */
    Object evaluate(IEvent theEvent);
    
    /**
     * <多流事件表达式求值>
     */
    Object evaluate(IEvent[] eventsPerStream);
    
    /**
     * <返回表达式返回类型>
     * <功能详细描述>
     */
    Class< ? > getType();
}
