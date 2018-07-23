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

package com.huawei.streaming.api.opereators;

import java.util.concurrent.TimeUnit;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.api.opereators.serdes.SimpleSerDeAPI;
import com.huawei.streaming.config.StreamingConfig;

/**
 * 生成随机数的算子
 * 
 */
public class RandomGenInputOperator extends InnerInputSourceOperator
{
    /**
     * 数据分发时间单位
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_HEADSTREAM_TIMEUNIT)
    private TimeUnit timeUnit;
    
    /**
     * 时间周期
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_HEADSTREAM_PERIOD)
    private Integer period;
    
    /**
     * 每时间周期发送事件数据，默认1个。即发送数据 1个/秒
     * 时间单位依照timeUnit中定义
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_HEADSTREAM_EVENTNUMPERPERIOD)
    private Integer eventNumPerPeriod;
    
    /**
     * 是否定期发送
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_HEADSTREAM_ISSCHEDULE)
    private Boolean isSchedule;
    
    /**
     *  发送个数，如果为0表示个数无限，配置参数
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_HEADSTREAM_TOTALNUMBER)
    private Integer totalNumber;
    
    /**
     * 第一个事件的延迟发送时间
     * 即延迟多久之后才开始生成数据
     * 
     * 时间单位同timeUnit
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_HEADSTREAM_DELAYTIME)
    private Long delayTime;
    
    /**
     * <默认构造函数>
     */
    public RandomGenInputOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    /**
     * 一次性发送一批数据
     */
    public static RandomGenInputOperator sendData(String id, int parallelNumber, Integer number, Long delayTime)
    {
        RandomGenInputOperator op = new RandomGenInputOperator(id, parallelNumber);
        op.setTimeUnit(TimeUnit.MILLISECONDS);
        op.setTotalNumber(number);
        op.setDelayTime(delayTime);
        op.setSchedule(false);
        op.setDeserializer(new SimpleSerDeAPI());
        return op;
    }
    
    /**
     * 设置每秒发送多少条数据
     */
    public static RandomGenInputOperator sendEverySeconds(String id, int parallelNumber, Integer number, Long delayTime)
    {
        return sendInterval(id, parallelNumber, TimeUnit.SECONDS, 1, number, 0, delayTime);
    }
    
    /**
     * 设置每分钟发送多少条数据
     */
    public static RandomGenInputOperator sendEveryMinutes(String id, int parallelNumber, Integer number, Long delayTime)
    {
        return sendInterval(id, parallelNumber, TimeUnit.MINUTES, 1, number, 0, delayTime);
    }
    
    /**
     * 设置每秒发送多少条数据，总共发送多少条数据
     */
    public static RandomGenInputOperator sendEverySecondsWithLimit(String id, int parallelNumber, Integer number,
        Integer totalNum, Long delayTime)
    {
        return sendInterval(id, parallelNumber, TimeUnit.SECONDS, 1, number, totalNum, delayTime);
    }
    
    /**
     * 设置每分钟发送多少条数据，总共发送多少条数据
     */
    public static RandomGenInputOperator sendEveryMinutesWithLimit(String id, int parallelNumber, Integer number,
        Integer totalNum, Long delayTime)
    {
        return sendInterval(id, parallelNumber, TimeUnit.MINUTES, 1, number, totalNum, delayTime);
    }
    
    /**
     * 设置每分钟发送多少条数据，总共发送多少条数据
     */
    public static RandomGenInputOperator sendInterval(String id, int parallelNumber, TimeUnit timeunit, Integer period,
        Integer eventNumPerPeriod, Integer totalNumber, Long delayTime)
    {
        RandomGenInputOperator op = new RandomGenInputOperator(id, parallelNumber);
        op.setTimeUnit(timeunit);
        op.setDelayTime(delayTime);
        op.setSchedule(true);
        op.setPeriod(period);
        op.setTotalNumber(totalNumber);
        op.setEventNumPerPeriod(eventNumPerPeriod);
        op.setDeserializer(new SimpleSerDeAPI());
        return op;
    }
    
    public TimeUnit getTimeUnit()
    {
        return timeUnit;
    }
    
    public void setTimeUnit(TimeUnit timeUnit)
    {
        this.timeUnit = timeUnit;
    }
    
    public Integer getPeriod()
    {
        return period;
    }
    
    public void setPeriod(Integer period)
    {
        this.period = period;
    }
    
    public Integer getEventNumPerPeriod()
    {
        return eventNumPerPeriod;
    }
    
    public void setEventNumPerPeriod(Integer eventNumPerPeriod)
    {
        this.eventNumPerPeriod = eventNumPerPeriod;
    }
    
    public Boolean isSchedule()
    {
        return isSchedule;
    }
    
    public void setSchedule(Boolean isschedue)
    {
        this.isSchedule = isschedue;
    }
    
    public Integer getTotalNumber()
    {
        return totalNumber;
    }
    
    public void setTotalNumber(Integer totalNumber)
    {
        this.totalNumber = totalNumber;
    }
    
    public Long getDelayTime()
    {
        return delayTime;
    }
    
    public void setDelayTime(Long delayTime)
    {
        this.delayTime = delayTime;
    }
    
}
