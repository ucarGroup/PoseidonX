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
import com.huawei.streaming.process.agg.resultmerge.IAggResultSetMerge;
import com.huawei.streaming.process.join.IJoinComposer;
import com.huawei.streaming.process.join.IJoinSetProcessor;
import com.huawei.streaming.process.join.JoinFilterProcessor;
import com.huawei.streaming.processor.JoinProcessor;
import com.huawei.streaming.view.FirstLevelStream;
import com.huawei.streaming.view.JoinProcessView;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.LengthSlideWindow;

/**
 * 自Join算子
 * 
 */
public class SelfJoinFunctionOp extends FunctionOperator
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 8825931094864211974L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(SelfJoinFunctionOp.class);
    
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
    
    /**
     * 左流的入口
     */
    private FirstLevelStream leftStream = new FirstLevelStream();
    
    /**
     * 右流的入口
     */
    private FirstLevelStream rightStream = new FirstLevelStream();
    
    /**
     * 左流名
     */
    private String leftStreamName;
    
    /**
     * 右流名
     */
    private String rightStreamName;
    
    /**
     * SelfJoin默认为单向触发，即一条事件触发一次JOIN
     */
    private boolean unidirectional = true;
    
    private int uniStreamIndex = 0;
    
    private OutputStorm outputStorm;
    
    /**
     * 输出类型
     */
    private OutputType outType = OutputType.I;
    
    /**
     * <默认构造函数>
     */
    public SelfJoinFunctionOp(IWindow leftWindow, IWindow rightWindow, IJoinComposer joinComposer,
        JoinFilterProcessor filterProcess, IAggResultSetMerge setProcessor)
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
     * <默认构造函数>
     */
    public SelfJoinFunctionOp(IWindow leftWindow, IWindow rightWindow, IJoinComposer joinComposer,
        JoinFilterProcessor filterProcess, IAggResultSetMerge setProcessor, OutputType type)
    {
        this(leftWindow, rightWindow, joinComposer, filterProcess, setProcessor);
        if (type != null)
        {
            this.outType = type;
        } 
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig config) throws StreamingException
    {
        super.setConfig(config);
        this.leftStreamName = (String)config.get(StreamingConfig.OPERATOR_SELFJOIN_INNER_LEFT_INPUT_STREAM_NAME);
        this.rightStreamName = (String)config.get(StreamingConfig.OPERATOR_SELFJOIN_INNER_RIGHT_INPUT_STREAM_NAME);
        if (config.containsKey(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL))
        {
            this.unidirectional = (Boolean)config.get(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL);
            if (config.containsKey(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL_INDEX))
            {
                this.uniStreamIndex = (Integer)config.get(StreamingConfig.OPERATOR_SELFJOIN_INNER_UNIDIRECTIONAL_INDEX);
            }
        }
        
        this.addInputSchema(leftStreamName,
                StreamingUtils.deSerializeSchema((String)config.get(StreamingConfig.OPERATOR_SELFJOIN_INNER_INPUT_SCHEMA)));
        this.addInputSchema(rightStreamName,
            StreamingUtils.deSerializeSchema((String)config.get(StreamingConfig.OPERATOR_SELFJOIN_INNER_INPUT_SCHEMA)));
        
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
        JoinProcessor joinprocessor =
            new JoinProcessor(joinComposer, new String[] {leftStreamName, rightStreamName}, filterProcessor,
                setProcessor, outputStorm, outType);
        //确定为单流JOIN
        joinprocessor.setSelfJoin(true);
        if (this.unidirectional)
        {
            joinprocessor.setUnidirectional(this.unidirectional);
            joinprocessor.setUniStreamIndex(uniStreamIndex);
        }
        joinView.setProcessor(joinprocessor);
        leftWindow.addView(joinView);
        rightWindow.addView(joinView);
        leftStream.addView(leftWindow);
        rightStream.addView(rightWindow);
        leftStream.start();
        rightStream.start();
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        LOG.debug("Join Function enter.");
        //如果为单向
        if (this.unidirectional)
        {
            //左流触发
            if (this.uniStreamIndex == 0)
            {
                //先更改流名称为右流名称，向右流发送数据
                event.setStreamName(this.rightStreamName);
                rightStream.add(event);
                //再更改流名称为左流名称，向左流发送数据
                event.setStreamName(this.leftStreamName);
                leftStream.add(event);
            }
            //右流触发
            else
            {
                //先更改流名称为左流名称，向左流发送数据
                event.setStreamName(this.leftStreamName);
                leftStream.add(event);
                //再更改流名称为右流名称，向右流发送数据
                event.setStreamName(this.rightStreamName);
                rightStream.add(event);
            }
        }
        //非单向
        else
        {
            //TODO
            //暂不支持非单向SELFJOIN
            LOG.debug("SelfJoin operator support unidirection only.");
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
