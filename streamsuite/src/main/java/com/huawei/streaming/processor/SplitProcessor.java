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

package com.huawei.streaming.processor;

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.common.Pair;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.expression.IBooleanExpression;
import com.huawei.streaming.output.IOutput;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.SelectSubProcess;

/**
 * <分割处理器>
 * <功能详细描述>
 * 
 */
public class SplitProcessor extends ProcessorImpl
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -433694961277537486L;
    
    /**
     * 
     * <分割信息类， 包含条件表达式和输出表达式信息>
     * <功能详细描述>
     * 
     */
    public class Split
    {
        /**
         * 条件表达式
         */
        private IBooleanExpression condition;
        
        /**
         * 输出表达式
         */
        private SelectSubProcess selector;
        
        /**
         * <默认构造函数>
         *@param condition 条件表达式
         *@param selector 输出表达式
         */
        public Split(IBooleanExpression condition, SelectSubProcess selector)
        {
            this.condition = condition;
            this.selector = selector;
        }
        
        /**
         * 返回 condition
         */
        public IBooleanExpression getCondition()
        {
            return condition;
        }
        
        /**
         * 返回 selector
         */
        public SelectSubProcess getSelector()
        {
            return selector;
        }
        
    }
    
    private final Split[] splits;
    
    private final IOutput output;
    
    private final OutputType type;
    
    /**
     * <默认构造函数>
     *@param splits 分割信息
     *@param output 输出对象
     *@param type   输出类型
     */
    public SplitProcessor(Split[] splits, IOutput output, OutputType type)
    {
        this.splits = splits;
        this.output = output;
        this.type = type;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void process(IEvent[] newData, IEvent[] oldData)
    {
        IBooleanExpression condition = null;
        SelectSubProcess selector = null;
        for (int i = 0; i < splits.length; i++)
        {
            condition = splits[i].getCondition();
            selector = splits[i].getSelector();
            
            IEvent[] newresult = getSelectEvents(condition, selector, newData);
            IEvent[] oldresult = null;
            if (type != OutputType.I)
            {
                oldresult = getSelectEvents(condition, selector, oldData);
            }
            
            Pair<IEvent[], IEvent[]> result = new Pair<IEvent[], IEvent[]>(newresult, oldresult);
            
            output.output(result);
        }
    }
    
    /**
     * <得到选择结果>
     */
    private IEvent[] getSelectEvents(IBooleanExpression condition, SelectSubProcess select, IEvent[] theData)
    {
        if (theData == null)
        {
            return null;
        }
        
        List<IEvent> qualified = new ArrayList<IEvent>();
        Boolean pass = null;
        for (IEvent e : theData)
        {
            pass = (Boolean)condition.evaluate(e);
            if (null != pass && pass)
            {
                qualified.add(e);
            }
        }
        
        if (qualified.size() > 0)
        {
            IEvent[] selectresult = select.process(qualified.toArray(new IEvent[qualified.size()]));
            
            return selectresult;
        }
        else
        {
            return null;
        }
        
    }
}
