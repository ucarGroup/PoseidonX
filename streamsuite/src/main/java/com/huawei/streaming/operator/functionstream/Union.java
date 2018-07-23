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

import java.io.Serializable;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.event.TupleEvent;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;

/**
 * 多流数据合并为单流
 * <功能详细描述>
 *
 */
public class Union implements Serializable
{
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -3722532035295186308L;
    
    private static final Logger LOG = LoggerFactory.getLogger(Union.class);
    
    /**
     * 输出流名称
     */
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
     * <默认构造函数>
     *
     */
    public Union(String name, IEventType schema, Map<String, IExpression[]> out)
        throws StreamingException
    {
        if (StringUtils.isEmpty(name))
        {
            LOG.error("Output stream name is null in union operator.");
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        if (null == schema)
        {
            LOG.error("Schema is null in union operator.");
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        if (null == out)
        {
            LOG.error("Output expressions is null in union operator.");
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        
        this.setOutStreamName(name);
        this.setOutSchema(schema);
        this.setOutSelect(out);
    }
    
    /**
     * 根据输入流名称及定义的选择字段，输出结果事件
     * <功能详细描述>
     *
     */
    public IEvent unionEvent(IEvent event)
    {
        String name = event.getStreamName();
        //根据事件名称确定取值规则
        if (null != name)
        {
            IExpression[] exp = getOutSelect().get(name);
            if (null != exp && exp.length > 0)
            {
                Object[] value = new Object[exp.length];
                for (int i = 0; i < exp.length; i++)
                {
                    value[i] = exp[i].evaluate(event);
                }
                
                return new TupleEvent(getOutStreamName(), getOutSchema(), value);
                
            }
        }
        return null;
    }
    
    public String getOutStreamName()
    {
        return outStreamName;
    }
    
    public void setOutStreamName(String outStreamName)
    {
        this.outStreamName = outStreamName;
    }
    
    public IEventType getOutSchema()
    {
        return outSchema;
    }
    
    public void setOutSchema(IEventType outSchema)
    {
        this.outSchema = outSchema;
    }
    
    public Map<String, IExpression[]> getOutSelect()
    {
        return outSelect;
    }
    
    public void setOutSelect(Map<String, IExpression[]> outSelect)
    {
        this.outSelect = outSelect;
    }
    
}
