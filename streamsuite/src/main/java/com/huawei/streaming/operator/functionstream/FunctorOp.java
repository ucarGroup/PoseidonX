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
 * 文 件 名:  FuntorOp.java
 * 版 本 号:  V1.0.0
 * 版    权:  Huawei Technologies Co., Ltd. Copyright 1988-2008,  All rights reserved
 * 描    述:  <描述>
 * 作    者:  j00199894
 * 创建日期:  2013-7-11
 */
package com.huawei.streaming.operator.functionstream;

import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
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
 * Functor功能算子
 */
public class FunctorOp extends FunctionOperator
{
    private static final long serialVersionUID = -9115269739974301889L;
    
    private SelectSubProcess selectorProcess;
    
    private IExpression filterExpression;
    
    private FirstLevelStream firststream = new FirstLevelStream();
    
    private OutputStorm outputStorm;
    
    /**
     * 有参构造函数
     */
    public FunctorOp(SelectSubProcess selector, IEventType tupleEventType, IExpression fiterExp)
    {
        this.selectorProcess = selector;
        this.filterExpression = fiterExp;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        ViewImpl filter = null;
        if (filterExpression != null)
        {
            filter = new FilterView(filterExpression);
        }
        else
        {
            filter = new FunctorView();
        }
        ProcessView processview = new ProcessView();
        outputStorm = new OutputStorm();
        outputStorm.setEmit(getEmitter());
        SimpleOutputProcessor simple = new SimpleOutputProcessor(selectorProcess, null, outputStorm, OutputType.I);
        processview.setProcessor(simple);
        filter.addView(processview);
        firststream.addView(filter);
        firststream.start();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        firststream.add(event);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        firststream.stop();
    }
}
