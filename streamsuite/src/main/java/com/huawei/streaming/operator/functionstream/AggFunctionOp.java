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

import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.output.OutputStorm;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.agg.resultmerge.IAggResultSetMerge;
import com.huawei.streaming.process.agg.resultmerge.IResultSetMerge;
import com.huawei.streaming.processor.AggregateProcessor;
import com.huawei.streaming.view.FilterView;
import com.huawei.streaming.view.FirstLevelStream;
import com.huawei.streaming.view.MergeView;
import com.huawei.streaming.view.ProcessView;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.group.IGroupWindow;

/**
 * aggregate算子
 */
public class AggFunctionOp extends FunctionOperator
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 6844442291453119711L;
    
    /**
     * 算子的第一级操作
     */
    private FirstLevelStream firstStream = new FirstLevelStream();
    
    /**
     * 算子的窗口
     */
    private IWindow window;
    
    /**
     * 结果处理
     */
    private IResultSetMerge resultMerge;
    
    /**
     * 过滤操作，出窗口后聚合前的过滤
     */
    private FilterView filterView;
    
    private OutputStorm outputStorm;
    
    /**
     * 输出类型
     */
    private OutputType outType = OutputType.I;
    
    /**
     * <默认构造函数>
     */
    public AggFunctionOp(IWindow window, FilterView filterView, IAggResultSetMerge resultMerge)
    {
        if (resultMerge == null)
        {
            throw new IllegalArgumentException("Aggregate Result Process is Null.");
        }
        
        this.window = window;
        this.filterView = filterView;
        this.resultMerge = resultMerge;
         
    }
    
    /**
     * <默认构造函数>
     */
    public AggFunctionOp(IWindow window, FilterView filterView, IAggResultSetMerge resultMerge, OutputType type)
    {
        this(window, filterView, resultMerge);
        if (type != null)
        {
            this.outType = type;
        }   
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        outputStorm = new OutputStorm(outType);
        outputStorm.setEmit(getEmitter());
        AggregateProcessor agg = new AggregateProcessor(resultMerge, outputStorm, outType);
        ProcessView processView = new ProcessView();
        processView.setProcessor(agg);
        
        if (window != null)
        {
            //groupWindow后面需要先添加mergeView
            if (window instanceof IGroupWindow)
            {
                MergeView mergeView = new MergeView();
                mergeView.addView(processView);
                
                if (null != filterView)
                {
                    window.addView(filterView);
                    filterView.addView(mergeView);
                }
                else
                {
                    window.addView(mergeView);
                }
            }
            else
            {
                if (null != filterView)
                {
                    window.addView(filterView);
                    filterView.addView(processView);
                }
                else
                {
                    window.addView(processView);
                }
            }
            firstStream.addView(window);
        }
        else
        {
            if (null != filterView)
            {
                firstStream.addView(filterView);
                filterView.addView(processView);
            }
            else
            {
                firstStream.addView(processView);
            }
        }
        
        firstStream.start();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        firstStream.add(event);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        firstStream.stop();
    }
}
