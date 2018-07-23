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

import com.huawei.streaming.util.StreamingUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.output.OutputStorm;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.join.IJoinComposer;
import com.huawei.streaming.process.join.IJoinSetProcessor;
import com.huawei.streaming.process.join.JoinFilterProcessor;
import com.huawei.streaming.processor.JoinProcessor;
import com.huawei.streaming.view.FirstLevelStream;
import com.huawei.streaming.view.JoinProcessView;
import com.huawei.streaming.view.MergeView;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.LengthSlideWindow;
import com.huawei.streaming.window.group.IGroupWindow;

/**
 * join算子
 */
public class JoinFunctionOp extends FunctionOperator
{
    
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -7850508838154121841L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(JoinFunctionOp.class);
    
    /**
     * 左流的入口
     */
    protected FirstLevelStream leftStream = new FirstLevelStream();
    
    /**
     * 右流的入口
     */
    protected FirstLevelStream rightStream = new FirstLevelStream();
    
    /**
     * 左流名
     */
    protected String leftStreamName;
    
    /**
     * 右流名
     */
    protected String rightStreamName;
    
    protected OutputStorm outputStorm;
    
    /**
     * 左流的窗口信息，一般为滑动窗口
     */
    private IWindow leftWindow;
    
    /**
     * 右流的窗口信息，一般为滑动窗口
     */
    private IWindow rightWindow;
    
    /**
     * 左右流关联服务
     */
    private IJoinComposer joinComposer;
    
    /**
     * 结果过滤服务
     */
    private JoinFilterProcessor filterProcessor;
    
    /**
     * 结果选择服务
     */
    private IJoinSetProcessor setProcessor;
    
    private boolean unidirectional = false;
    
    private int uniStreamIndex = 0;
    
    /**
     * 输出类型
     */
    private OutputType outType = OutputType.I;
    
    /**
     * 
     * <默认构造函数>
     */
    public JoinFunctionOp(IWindow leftWindow, IWindow rightWindow, IJoinComposer joinComposer,
        JoinFilterProcessor filterProcess, IJoinSetProcessor setProcessor)
    {
        if (leftWindow == null)
        {
            this.leftWindow = new LengthSlideWindow(1);
        }
        else
        {
            this.leftWindow = leftWindow;
        }
        
        if (rightWindow == null)
        {
            this.rightWindow = new LengthSlideWindow(1);
        }
        else
        {
            this.rightWindow = rightWindow;
        }
        
        this.joinComposer = joinComposer;
        this.filterProcessor = filterProcess;
        this.setProcessor = setProcessor;   
    }
    
    /**
     * 
     * <默认构造函数>
     */
    public JoinFunctionOp(IWindow leftWindow, IWindow rightWindow, IJoinComposer joinComposer,
        JoinFilterProcessor filterProcess, IJoinSetProcessor setProcessor, OutputType type)
    {
        this(leftWindow, rightWindow, joinComposer, filterProcess, setProcessor);
        if (type != null)
        {
            this.outType = type;
        }   
    }
    
    /**
     * 从配置文件中获得算子本身需要的配置数据
     */
    @Override
    public void setConfig(StreamingConfig config) throws StreamingException
    {
        super.setConfig(config);
        this.leftStreamName = (String)config.get(StreamingConfig.OPERATOR_JOIN_INNER_LEFT_INPUT_STREAM_NAME);
        this.rightStreamName = (String)config.get(StreamingConfig.OPERATOR_JOIN_INNER_RIGHT_INPUT_STREAM_NAME);
        if (config.containsKey(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL))
        {
            this.unidirectional = (Boolean)config.get(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL);
            if (config.containsKey(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL_INDEX))
            {
                this.uniStreamIndex = (Integer)config.get(StreamingConfig.OPERATOR_JOIN_INNER_UNIDIRECTIONAL_INDEX);
            }
        }
        
        this.addInputStream(leftStreamName);
        this.addInputStream(rightStreamName);
        this.addInputSchema(leftStreamName, StreamingUtils.deSerializeSchema((String)config.get(StreamingConfig.OPERATOR_JOIN_INNER_LEFT_SCHEMA)));
        this.addInputSchema(rightStreamName, StreamingUtils.deSerializeSchema((String)config.get(StreamingConfig.OPERATOR_JOIN_INNER_RIGHT_SCHEMA)));
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        JoinProcessView joinView = new JoinProcessView();
        outputStorm = new OutputStorm(outType);
        outputStorm.setEmit(getEmitter());
        String[] names = new String[] {leftStreamName, rightStreamName};
        JoinProcessor joinprocessor =
            new JoinProcessor(joinComposer, names, filterProcessor, setProcessor, outputStorm, outType);
        if (this.unidirectional)
        {
            joinprocessor.setUnidirectional(this.unidirectional);
            joinprocessor.setUniStreamIndex(uniStreamIndex);
        }
        joinView.setProcessor(joinprocessor);
        
        initJoinWindow(joinView, leftWindow, leftStream);
        initJoinWindow(joinView, rightWindow, rightStream);
        
        leftStream.start();
        rightStream.start();
        
    }
    
    private void initJoinWindow(JoinProcessView joinView, IWindow window, FirstLevelStream stream)
    {
        if (window != null)
        {
            //groupWindow后面需要先添加mergeView
            if (window instanceof IGroupWindow)
            {
                MergeView mergeView = new MergeView();
                mergeView.addView(joinView);
                window.addView(mergeView);
            }
            else
            {
                window.addView(joinView);
            }
            stream.addView(window);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        LOG.debug("Join Function enter.");
        if (streamName.equals(leftStreamName))
        {
            leftStream.add(event);
        }
        else if (streamName.equals(rightStreamName))
        {
            rightStream.add(event);
        }
        else
        {
            LOG.debug("The tuple's streamName is invalid,streamName={}.", event.getStreamName());
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        leftStream.stop();
        rightStream.stop();
    }
    
}
