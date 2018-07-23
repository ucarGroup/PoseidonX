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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.datasource.DataSourceContainer;
import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.output.OutputType;
import com.huawei.streaming.process.join.CrossBiJoinComposer;
import com.huawei.streaming.process.join.IJoinComposer;
import com.huawei.streaming.process.join.IJoinSetProcessor;
import com.huawei.streaming.process.join.JoinFilterProcessor;
import com.huawei.streaming.window.EventBasedWindow;
import com.huawei.streaming.window.IWindow;

/**
 * 数据源（数据库）查询计算算子，数据会触发查询数据库操作，并对返回结果进行计算
 * 通过join的方式实现，左流就为自然发送来的数据流，根据左流的数据到数据库中查询出相关的数据并将数据发送到右流中，
 * 即为左crossjoin
 */
public class DataSourceFunctionOp extends JoinFunctionOp
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 1499564805532907613L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceFunctionOp.class);
    
    /**
     * 数据库操作对象（右流）
     */
    private DataSourceContainer dataSource;
    
    /**
     * 数据库查询表达式
     */
    private Map<String, IExpression> cqlExpressions;
    
    private CrossBiJoinComposer crossJoinComposer;
    
    /**
     * <默认构造函数>
     */
    public DataSourceFunctionOp(IWindow leftWindow, DataSourceContainer dataSource, Map<String, IExpression> cqlExpressions,
        IJoinComposer joinComposer, JoinFilterProcessor filterProcess, IJoinSetProcessor setProcessor)
    {
        this(leftWindow, dataSource, cqlExpressions,joinComposer,filterProcess,setProcessor, OutputType.I);
    }
    
    /**
     * <默认构造函数>
     */
    public DataSourceFunctionOp(IWindow leftWindow, DataSourceContainer dataSource, Map<String, IExpression> cqlExpressions,
        IJoinComposer joinComposer, JoinFilterProcessor filterProcess, IJoinSetProcessor setProcessor, OutputType type)
    {
        //此处需要构造EventBasedWindow，来模拟右流的窗口
        super(leftWindow, new EventBasedWindow(), joinComposer, filterProcess, setProcessor, type);
        this.dataSource = dataSource;
        this.cqlExpressions = cqlExpressions;
        
        if (!(joinComposer instanceof CrossBiJoinComposer))
        {
            String msg = "Only cross join composer is allowed in datasource function operator.";
            LOG.error(msg);
            throw new StreamingRuntimeException(msg);
        }
        crossJoinComposer = (CrossBiJoinComposer)joinComposer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig config) throws StreamingException
    {
        super.setConfig(config);
        super.rightStreamName = crossJoinComposer.getRightStream().getStreamName();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
     throws StreamingException
    {
        super.initialize();
        dataSource.initialize();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(String streamName, TupleEvent event)
        throws StreamingException
    {
        LOG.debug("Join Function enter.");
        if (streamName.equals(super.leftStreamName))
        {
            //查询数据库，并将结果发送到右流
            evaluateDataSource(event);
            
            //由于是左crossjoin，因此需要先查询数据库，再添加左流事件，否则左流事件到右流join时右流还没有事件
            super.leftStream.add(event);
        }
        else if (streamName.equals(super.rightStreamName))
        {
            super.rightStream.add(event);
        }
        else
        {
            LOG.warn("The tuple's streamName is invalid,streamName={}.", event.getStreamName());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws StreamingException
    {
        super.destroy();
        dataSource.destroy();
    }

    /**
     * 计算数据库查询结果，并将数据发送到右流中，发送完成后再发送一个标识事件
     */
    private void evaluateDataSource(TupleEvent event)
        throws StreamingException
    {
        Map<String, Object> cqlExpressionValues = new HashMap<String, Object>();
        if (null != cqlExpressions)
        {
            for (String key : cqlExpressions.keySet())
            {
                cqlExpressionValues.put(key, cqlExpressions.get(key).evaluate(event));
            }
        }
        
        //循环发送查询结果
        List<Object[]> dataSourceResult = dataSource.evaluate(cqlExpressionValues);
        for (int i = 0; i < dataSourceResult.size(); i++)
        {
            super.rightStream.add(new TupleEvent(super.rightStreamName, dataSource.getEventType(),
                dataSourceResult.get(i)));
        }
        
        //发送标识事件
        IEvent flagEvent = new TupleEvent();
        flagEvent.setFlagEvent();
        super.rightStream.add(flagEvent);
    }
    

}
