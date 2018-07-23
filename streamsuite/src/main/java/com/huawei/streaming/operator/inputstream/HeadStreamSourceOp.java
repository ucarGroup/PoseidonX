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

package com.huawei.streaming.operator.inputstream;

import java.io.Serializable;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.common.RandomValueGen;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.event.Attribute;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.IEmitter;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.serde.StreamSerDe;

/**
 * 自主随机发送数据的数据源算子
 * <功能详细描述>
 * 
 */
public class HeadStreamSourceOp implements IInputStreamOperator
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 4660203255085836607L;
    
    private static final Logger LOG = LoggerFactory.getLogger(HeadStreamSourceOp.class);
    
    private static final int MAX_TIME = 1000000000;
    
    private transient HeadStream headStream;
    
    /**
     * 时间单位，默认秒,配置参数
     */
    private TimeUnit timeUnit = TimeUnit.SECONDS;
    
    /**
     * 时间周期，默认1秒，配置参数
     */
    private int period = 1;
    
    /**
     * 每时间周期发送事件数据，默认1个。即发送数据 1个/秒，配置参数
     */
    private int eventNumPerPeriod = 1;
    
    /**
     * 是否定期发送，配置参数
     */
    private boolean isSchedue = false;
    
    /**
     * 间隔时间，如果为0表示没有间隔
     */
    private long interval = 0;
    
    /**
     * 发送个数，如果为0表示个数无限，配置参数
     */
    private int totalNum = 0;
    
    /**
     * 延迟发送时间，如果为0表示立即发送，配置参数
     */
    private long delay = 0;
    
    private transient ScheduledExecutorService scheduler;
    
    /**
     * 发送次数计数器
     */
    private int counts = 0;
    
    private IEmitter emitter;
    
    private StreamSerDe serde;
    
    private StreamingConfig config;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        config = conf;
        initParameters(conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamingConfig getConfig()
    {
        return config;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize()
        throws StreamingException
    {
        LOG.info("Init HeadStreamSource Operator...");
        this.headStream = new HeadStream(serde.getSchema());
        if (delay > 0)
        {
            LOG.info("Preparing send... delay={}", delay);
            try
            {
                Thread.sleep(delay);
            }
            catch (InterruptedException e)
            {
                LOG.error("HeadStream operator delay error", e);
                throw new RuntimeException(e);
            }
        }
        
    }
    
    private static class HeadStreamThreadFactory implements ThreadFactory
    {
        // set new thread as daemon thread and name appropriately
        public Thread newThread(Runnable r)
        {
            Thread t = new Thread(r, HeadStreamSourceOp.class.getName());
            t.setDaemon(true);
            return t;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void execute()
        throws StreamingException
    {
        /**
         * 周期发送数据
         */
        if (isSchedue && scheduler == null)
        {
            
            LOG.info("Init scheduler...");
            scheduler = Executors.newSingleThreadScheduledExecutor(new HeadStreamThreadFactory());
            
            ScheduleRunner emitTask = new ScheduleRunner();
            emitTask.setEmitter(emitter);
            
            //每时间单位发送数
            if (eventNumPerPeriod == period)
            {
                interval = 1;
            }
            else if (eventNumPerPeriod > period)
            {
                //数据量过大，无法转换
                if (period * MAX_TIME < eventNumPerPeriod)
                {
                    LOG.error("Too huge event num, not supported. event num={}", eventNumPerPeriod);
                    throw new RuntimeException("Too huge event num, not supported.");
                }
                //当前周期转换为最小的微秒时间单位
                interval = ((long)period * MAX_TIME) / this.eventNumPerPeriod;
                timeUnit = TimeUnit.NANOSECONDS;
            }
            else
            {
                interval = period / eventNumPerPeriod;
            }
            
            LOG.info("Launch scheduler... fixed Interval={}, delay={}", interval, delay);
            scheduler.scheduleAtFixedRate(emitTask, delay, interval, timeUnit);
        }
        
        if (isSchedue && scheduler != null)
        {
            return;
        }
        
        /**
         * 无周期发送数据要求,每次调用netTuple,发送一条数据
         */
        
        //无限制发送数据
        if (totalNum == 0)
        {
            emitter.emit(headStream.getOutput());
        }
        else
        {
            if (counts < totalNum)
            {
                emitter.emit(headStream.getOutput());
                counts++;
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy()
        throws StreamingException
    {
        if (null != scheduler)
        {
            scheduler.shutdown();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setEmitter(IEmitter iEmitter)
    {
        this.emitter = iEmitter;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSerDe(StreamSerDe streamSerDe)
    {
        this.serde = streamSerDe;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public StreamSerDe getSerDe()
    {
        return serde;
    }

    /**
     * 随机数发送对象体
     * 
     */
    public static class HeadStream
    {
        private IEventType eventType;
        
        private RandomValueGen randomGen = new RandomValueGen();
        
        /**
         * <默认构造函数>
         *@param eType 输出eType
         */
        public HeadStream(IEventType eType)
        {
            if (null == eType)
            {
                LOG.error("The output eventType of HeadStream is empty.");
                throw new RuntimeException("The output eventType of HeadStream is empty.");
            }

            this.eventType = eType;
        }
        
        /**
         * 获得一个生成的事件
         * <功能详细描述>
         */
        @SuppressWarnings("rawtypes")
        public Object[] getOutput()
        {
            Object[] values = new Object[eventType.getSize()];
            Attribute[] atts = eventType.getAllAttributes();
            
            Class cla = null;
            Attribute att = null;
            /**
             * 随机生成所需数据
             */

            for (int i = 0; i < atts.length; i++)
            {
                att = atts[i];
                cla = att.getAttDataType();
                values[i] = genRandomValue(cla);
            }

            return values;
        }
        
        @SuppressWarnings("rawtypes")
        private Object genRandomValue(Class cname)
        {
            if (cname.getName().endsWith("Integer"))
            {
                return randomGen.getInteger(-1, false);
            }
            else if (cname.getName().endsWith("Double"))
            {
                return randomGen.getDouble(-1, false);
            }
            else if (cname.getName().endsWith("Float"))
            {
                return randomGen.getFloat(-1, false);
            }
            else if (cname.getName().endsWith("Boolean"))
            {
                return randomGen.getBoolean();
            }
            else if (cname.getName().endsWith("String"))
            {
                return randomGen.getString("generated", true);
            }
            return randomGen.getInteger(-1, false);
        }
        
    }
    
    /**
     * 定时任务执行接口
     * 
     */
    public class ScheduleRunner implements Runnable
    {
        
        private IEmitter emitObject;
        
        /**
         * 设置emitter
         */
        public ScheduleRunner setEmitter(IEmitter emitobj)
        {
            this.emitObject = emitobj;
            return this;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void run()
        {
            //有限次数发送数据
            try
            {
                if (totalNum > 0)
                {
                    if (counts < totalNum)
                    {
                        emitObject.emit(headStream.getOutput());
                        counts++;
                    }
                }
                //无限次数发送数据
                else
                {
                    emitObject.emit(headStream.getOutput());
                }
            }
            catch (StreamingException e)
            {
                throw new RuntimeException(e);
            }
        }
        
    }
    
    private void initParameters(StreamingConfig conf)
        throws StreamingException
    {
        if (conf.containsKey(StreamingConfig.OPERATOR_HEADSTREAM_TIMEUNIT))
        {
            this.timeUnit = TimeUnit.valueOf(conf.getStringValue(StreamingConfig.OPERATOR_HEADSTREAM_TIMEUNIT));
        }
        
        if (conf.containsKey(StreamingConfig.OPERATOR_HEADSTREAM_PERIOD))
        {
            this.period = conf.getIntValue(StreamingConfig.OPERATOR_HEADSTREAM_PERIOD);
        }
        if (conf.containsKey(StreamingConfig.OPERATOR_HEADSTREAM_EVENTNUMPERPERIOD))
        {
            this.eventNumPerPeriod = conf.getIntValue(StreamingConfig.OPERATOR_HEADSTREAM_EVENTNUMPERPERIOD);
        }
        if (conf.containsKey(StreamingConfig.OPERATOR_HEADSTREAM_ISSCHEDULE))
        {
            this.isSchedue = conf.getBooleanValue(StreamingConfig.OPERATOR_HEADSTREAM_ISSCHEDULE);
        }
        if (conf.containsKey(StreamingConfig.OPERATOR_HEADSTREAM_TOTALNUMBER))
        {
            this.totalNum = conf.getIntValue(StreamingConfig.OPERATOR_HEADSTREAM_TOTALNUMBER);
        }
        if (conf.containsKey(StreamingConfig.OPERATOR_HEADSTREAM_DELAYTIME))
        {
            this.delay = conf.getLongValue(StreamingConfig.OPERATOR_HEADSTREAM_DELAYTIME);
        }
    }
    
}
