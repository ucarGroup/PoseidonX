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

package com.huawei.streaming.process.join;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IExpression;

/**
 * 
 * JOIN结果过滤处理
 * <功能详细描述>
 * 
 */
public class JoinFilterProcessor implements Serializable
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 5253815178159414153L;
    
    private static final Logger LOG = LoggerFactory.getLogger(JoinFilterProcessor.class);
    
    private IExpression expr;
    
    /**
     * <默认构造函数>
     *@param exp 过滤表达式
     */
    public JoinFilterProcessor(IExpression exp)
    {
        this.expr = exp;
    }
    
    /**
     * 根据表达式过滤
     * <功能详细描述>
     */
    public void filter(Set<MultiKey> eventsPerStream)
    {
        if (null == eventsPerStream || 0 == eventsPerStream.size() || expr == null)
        {
            return;
        }
        
        Iterator<MultiKey> iter = eventsPerStream.iterator();
        MultiKey composed;
        while (iter.hasNext())
        {
            composed = iter.next();
            IEvent[] events = (IEvent[])composed.getKeys();
            Object eval = expr.evaluate(events);
            if (eval == null || eval instanceof Boolean)
            {
                if (eval == null || !(Boolean)eval)
                {
                    iter.remove();
                }
            }
            else
            {
                LOG.error("The return value type of expression is not matched.");
                throw new RuntimeException("The return value type of expression is not matched.");
            }
        }
    }
    
    public IExpression getExpr()
    {
        return expr;
    }
}
