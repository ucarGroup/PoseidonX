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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.executor.expressioncreater.ConstExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.StreamingDataType;

/**
 * 常量表达式的描述
 *
 */
@ExpressionCreatorAnnotation(ConstExpressionCreator.class)
public class ConstExpressionDesc implements ExpressionDescribe
{
    private static final Logger LOG = LoggerFactory.getLogger(ConstExpressionDesc.class);

    /**
     * 常量值
     */
    private Object constValue;

    /**
     * 常量类型，字符串类型或者int类型
     */
    private Class< ? > type;

    /**
     * <默认构造函数>
     *
     */
    public ConstExpressionDesc(Object constValue, Class< ? > type)
    {
        super();
        this.constValue = constValue;
        this.type = type;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
	
	    //对应常量值为null的场景
        if (constValue == null)
        {
            return null;
        }

        StreamingDataType dataType = null;

        try
        {
            dataType = StreamingDataType.getStreamingDataType(type);
        }
        catch (StreamingException e)
        {
            LOG.warn("Ignore an StreamingExpression.");
            return "'" + constValue + "'";
        }

        String constStringValue = constValue.toString();
        String postfix = getConstPostfix(dataType);
        //没有时间类型的常量，统一按照字符串标准进行处理
        return postfix == null ? "'" + constValue + "'" : constStringValue + postfix;
    }

    public Object getConstValue()
    {
        return constValue;
    }

    public void setConstValue(Object constValue)
    {
        this.constValue = constValue;
    }

    public Class< ? > getType()
    {
        return type;
    }

    public void setType(Class< ? > type)
    {
        this.type = type;
    }

    private String getConstPostfix(StreamingDataType dataType)
    {
        //各种事件类型没有常量，统一按照字符串来处理
        //int和boolean没有后缀
        switch (dataType)
        {
            case INT:
            case BOOLEAN:
                return "";
            case LONG:
                return "L";
            case FLOAT:
                return "F";
            case DOUBLE:
                return "D";
            case DECIMAL:
                return "BD";
            default:
                return null;
        }
    }
}
