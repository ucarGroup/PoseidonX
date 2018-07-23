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

package com.huawei.streaming.operator.functionstream;

import java.util.List;
import java.util.Map;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.FunctionOperator;

/**
 * 会和算子，将具有相同主键的不同流中的数据合并为一条
 */
public class CombineFunctionOp extends FunctionOperator
{
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 51143348752914027L;
    
    /**
     * 会合对象，执行会合服务用
     */
    private Combine combine;
    
    /**
     * Map<流名称，所选择的属性表达式>
     */
    private Map<String, IExpression[]> outSelect;
    
    /**
     * Map<流名称，组合所使用的主键>
     */
    private Map<String, String> keyMap;
    
    /**
     * 输入流列表
     */
    private List<String> inputStreamNameList;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        this.combine = new Combine(getOutputStream(), getOutputSchema(), outSelect, inputStreamNameList, keyMap);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        IEvent unionevent = combine.unionEvent(event);
        if (null != unionevent)
        {
            getEmitter().emit(unionevent.getAllValues());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        // TODO Auto-generated method stub
        
    }
    
    /**
     * 设置配置属性
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setConfig(StreamingConfig config) throws StreamingException
    {
        super.setConfig(config);
        if (config.containsKey(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_EXPRESSION))
        {
            this.outSelect =
                (Map<String, IExpression[]>)config.get(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_EXPRESSION);
        }
        
        if (config.containsKey(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_KEY))
        {
            this.keyMap = (Map<String, String>)config.get(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES_AND_KEY);
        }
        if (config.containsKey(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES))
        {
            this.inputStreamNameList = (List<String>)config.get(StreamingConfig.OPERATOR_COMBINE_INPUTNAMES);
        }
        
    }
    
}
