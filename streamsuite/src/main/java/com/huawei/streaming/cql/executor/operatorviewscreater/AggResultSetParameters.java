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

package com.huawei.streaming.cql.executor.operatorviewscreater;

import java.util.List;
import java.util.Map;

import com.huawei.streaming.api.opereators.BasicAggFunctionOperator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.window.IWindow;

/**
 * ResultSetMerge实例创建需要的参数
 * 
 */
public class AggResultSetParameters
{
    private BasicAggFunctionOperator basicAggOperator;
    
    private EventTypeMng streamschema;
    
    private List<Schema> inputSchemas;
    
    private List<Schema> outputSchemas;
    
    private OperatorTransition transitionOut;
    
    private Map<String, IWindow> streamWindows;
    
    private IExpression expressionBeforeAggregate;
    
    private Map<String, String> systemConfig;
    
    private List<Window> operatorWindows;
    
    public BasicAggFunctionOperator getBasicAggOperator()
    {
        return basicAggOperator;
    }
    
    public void setBasicAggOperator(BasicAggFunctionOperator basicAggOperator)
    {
        this.basicAggOperator = basicAggOperator;
    }
    
    public EventTypeMng getStreamschema()
    {
        return streamschema;
    }
    
    public void setStreamschema(EventTypeMng streamschema)
    {
        this.streamschema = streamschema;
    }
    
    public List<Schema> getInputSchemas()
    {
        return inputSchemas;
    }
    
    public void setInputSchemas(List<Schema> inputSchemas)
    {
        this.inputSchemas = inputSchemas;
    }
    
    public List<Schema> getOutputSchemas()
    {
        return outputSchemas;
    }
    
    public void setOutputSchemas(List<Schema> outputSchemas)
    {
        this.outputSchemas = outputSchemas;
    }
    
    public OperatorTransition getTransitionOut()
    {
        return transitionOut;
    }
    
    public void setTransitionOut(OperatorTransition transitionOut)
    {
        this.transitionOut = transitionOut;
    }
    
    public Map<String, IWindow> getStreamWindows()
    {
        return streamWindows;
    }
    
    public void setStreamWindows(Map<String, IWindow> streamWindows)
    {
        this.streamWindows = streamWindows;
    }
    
    public IExpression getExpressionBeforeAggregate()
    {
        return expressionBeforeAggregate;
    }
    
    public void setExpressionBeforeAggregate(IExpression expressionBeforeAggregate)
    {
        this.expressionBeforeAggregate = expressionBeforeAggregate;
    }
    
    public Map<String, String> getSystemConfig()
    {
        return systemConfig;
    }
    
    public void setSystemConfig(Map<String, String> systemConfig)
    {
        this.systemConfig = systemConfig;
    }
    
    public List<Window> getOperatorWindows()
    {
        return operatorWindows;
    }
    
    public void setOperatorWindows(List<Window> operatorWindows)
    {
        this.operatorWindows = operatorWindows;
    }
    
}
