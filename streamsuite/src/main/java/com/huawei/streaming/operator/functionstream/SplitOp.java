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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.output.OutputStorm;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.processor.SimpleOutputProcessor;
import com.huawei.streaming.view.FilterView;
import com.huawei.streaming.view.FirstLevelStream;
import com.huawei.streaming.view.FunctorView;
import com.huawei.streaming.view.ProcessView;
import com.huawei.streaming.view.ViewImpl;

/**
 * 分割算子
 * 无状态操作，不支持窗口
 *
 */
public class SplitOp extends FunctionOperator
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 8165331175152418203L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(SplitOp.class);
    
    /**
     * 输出流名称及schema
     */
    private Map<String, IEventType> outputSchemas = null;
    
    /**
     * 输出流名称及对应的select子句
     */
    private Map<String, SelectSubProcess> selectorMap = null;
    
    /**
     * 输出流名称及对应的表达式
     */
    private Map<String, IExpression> filterMap = null;
    
    private List<FirstLevelStream> firstStreamList = new ArrayList<FirstLevelStream>();
    
    /**
     * <默认构造函数>
     *
     */
    public SplitOp(Map<String, SelectSubProcess> selector, Map<String, IExpression> filter,
        Map<String, IEventType> schema)
    {
        selectorMap = selector;
        filterMap = filter;
        outputSchemas = schema;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        if (filterMap == null || filterMap.size() == 0)
        {
            LOG.error("Arguments in '{}' operator is null.", this.getClass().getName());
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        String streamName = null;
        IExpression expr = null;
        ViewImpl filter = null;
        for (Map.Entry<String, IExpression> m : filterMap.entrySet())
        {
            streamName = m.getKey();
            expr = m.getValue();
            if (null == expr)
            {
                filter = new FunctorView();
            }
            else
            {
                filter = new FilterView(expr);
            }
            
            ProcessView processview = new ProcessView();
            OutputStorm outputStorm = new OutputStorm();
            outputStorm.setEmit(getEmitter(streamName));
            SimpleOutputProcessor simple =
                new SimpleOutputProcessor(selectorMap.get(streamName), null, outputStorm, OutputType.I);
            processview.setProcessor(simple);
            filter.addView(processview);
            FirstLevelStream firststream = new FirstLevelStream();
            firststream.addView(filter);
            firststream.start();
            firstStreamList.add(firststream);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        for (FirstLevelStream firststream : firstStreamList)
        {
            firststream.add(event);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        for (FirstLevelStream firststream : firstStreamList)
        {
            firststream.stop();
        }
    }
    
    public Map<String, IEventType> getOutputSchemaMap()
    {
        return this.outputSchemas;
    }
}
