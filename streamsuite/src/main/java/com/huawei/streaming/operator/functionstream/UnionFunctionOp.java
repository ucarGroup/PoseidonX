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

import java.util.Map;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.util.StreamingUtils;

/**
 * 
 * 多流数据合并为单流，支持不同输入schema的输入流
 * <功能详细描述>
 * 
 */
public class UnionFunctionOp extends FunctionOperator
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 3368341684052652566L;
    
    private Union union;
    
    private String outStreamName;
    
    /**
     * 输出流schema
     */
    private IEventType outSchema;
    
    /**
     * Map<流名称，所选择的属性表达式>
     */
    private Map<String, IExpression[]> outSelect;
    
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public void setConfig(StreamingConfig config) throws StreamingException
    {
        super.setConfig(config);
        if (config.containsKey(StreamingConfig.OPERATOR_UNION_INNER_INPUTNAMES_AND_EXPRESSION))
        {
            this.outSelect =
                (Map<String, IExpression[]>)config.get(StreamingConfig.OPERATOR_UNION_INNER_INPUTNAMES_AND_EXPRESSION);
        }
        if (config.containsKey(StreamingConfig.STREAMING_INNER_OUTPUT_STREAM_NAME))
        {
            this.outStreamName = (String)config.get(StreamingConfig.STREAMING_INNER_OUTPUT_STREAM_NAME);
        }
        if (config.containsKey(StreamingConfig.STREAMING_INNER_OUTPUT_SCHEMA))
        {
            this.outSchema = StreamingUtils.deSerializeSchema((String)config.get(StreamingConfig.STREAMING_INNER_OUTPUT_SCHEMA));
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        this.union = new Union(outStreamName, outSchema, outSelect);
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        IEvent unionevent = union.unionEvent(event);
        if(unionevent != null)
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
    
}
