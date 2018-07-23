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

package com.huawei.streaming.common;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.IllegalDataTypeException;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * <流处理中类型处理工具类>
 * <功能详细描述>
 *
 */
public class StreamClassUtil
{
    private static final int COUNT_INIT = 2;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamClassUtil.class);
    
    /**
     * <返回包装类>
     * <功能详细描述>
     *
     */
    public static Class< ? > getWrapType(Class< ? > type)
    {
        if (!type.isPrimitive())
        {
            return type;
        }
        
        if (type == boolean.class)
        {
            return Boolean.class;
        }
        
        if (type == int.class)
        {
            return Integer.class;
        }
        
        if (type == long.class)
        {
            return Long.class;
        }
        
        if (type == double.class)
        {
            return Double.class;
        }
        
        if (type == float.class)
        {
            return Float.class;
        }
        
        return type;
    }
    
    /**
     * <判断是否数字类型>
     * <功能详细描述>
     *
     */
    public static boolean isNumberic(Class< ? > type)
    {
        if ((type == Double.class) || (type == double.class))
        {
            return true;
        }
        
        if ((type == Float.class) || (type == float.class))
        {
            return true;
        }
        
        if ((type == Long.class) || (type == long.class))
        {
            return true;
        }
        
        if ((type == Integer.class) || (type == int.class))
        {
            return true;
        }
        
        if (type == BigDecimal.class)
        {
            return true;
        }
        
        return false;
    }
    
    /**
     * <根据算术运算的左右表达式类型，推测结果类型>
     * <功能详细描述>
     *
     */
    public static Class< ? > getArithmaticType(Class< ? > leftType, Class< ? > rightType)
        throws IllegalDataTypeException
    {
        Class< ? > leftWarp = getWrapType(leftType);
        Class< ? > rightWarp = getWrapType(rightType);
        
        if (!isNumberic(leftWarp) || !isNumberic(rightWarp))
        {
            LOG.error("Can't infer result type from {} and {}.", leftType.getName(), rightType.getName());
            IllegalDataTypeException exp = new IllegalDataTypeException(ErrorCode.SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE);
            throw exp;
        }
        
        if (leftWarp == rightWarp)
        {
            return leftWarp;
        }
        
        if ((leftWarp == BigDecimal.class) || (rightWarp == BigDecimal.class))
        {
            return BigDecimal.class;
        }
        
        if (leftWarp == Double.class || rightWarp == Double.class)
        {
            return Double.class;
        }
        
        if (leftWarp == Float.class || rightWarp == Float.class)
        {
            return Float.class;
        }
        
        if (leftWarp == Long.class || rightWarp == Long.class)
        {
            return Long.class;
        }
        
        return Integer.class;
    }
    
    /**
     * <根据一系列表达式类型，推测返回类型。例如case表达式>
     * <功能详细描述>
     *
     */
    public static Class< ? > getCommonType(List<Class< ? >> childTypes)
        throws IllegalDataTypeException
    {
        if (childTypes == null || childTypes.size() < 1)
        {
            LOG.error("Child Types must have one.");
            IllegalDataTypeException exp = new IllegalDataTypeException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exp;
        }
        
        int childSize = childTypes.size();
        
        if (childSize == 1)
        {
            return getWrapType(childTypes.get(0));
        }
        
        List<Class< ? >> wrapTypes = new LinkedList<Class< ? >>();
        for (int i = 0; i < childSize; i++)
        {
            if (childTypes.get(i) != null)
            {
                wrapTypes.add(getWrapType(childTypes.get(i)));
            }
        }
        
        int wrapSize = wrapTypes.size();
        
        if (wrapSize == 0)
        {
            return null;
        }
        if (wrapSize == 1)
        {
            return wrapTypes.get(0);
        }
        
        //如果第一个为String类型，则认为所有表达式都应该为String类型，结果类型为String类型
        if (wrapTypes.get(0) == String.class)
        {
            for (int i = 0; i < wrapSize; i++)
            {
                if (wrapTypes.get(i) != String.class)
                {
                    LOG.error("Can't infer result type from {} and {}.", childTypes.get(i), String.class);
                    IllegalDataTypeException exp = new IllegalDataTypeException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
                    throw exp;
                }
            }
            
            return String.class;
        }
        
        //如果第一个为Boolean类型，则认为所有表达式都应该为Boolean类型，结果类型为Boolean类型
        if (wrapTypes.get(0) == Boolean.class)
        {
            for (int i = 0; i < wrapSize; i++)
            {
                if (wrapTypes.get(i) != Boolean.class)
                {
                    LOG.error("Can't infer result type from {} and {}.", childTypes.get(i), Boolean.class);
                    IllegalDataTypeException exp = new IllegalDataTypeException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
                    throw exp;
                }
            }
            
            return Boolean.class;
        }
        
        Class< ? > result = getArithmaticType(wrapTypes.get(0), wrapTypes.get(1));
        int count = COUNT_INIT;
        while (count < wrapSize)
        {
            result = getArithmaticType(result, wrapTypes.get(count));
            count++;
        }
        
        return result;
    }
    
    /**
     * <将Number数值根据指定类型转化为对应类型的值>
     * <功能详细描述>
     *
     */
    public static Number getNumbericValueForType(Number result, Class< ? > resultType)
    {
        if (result == null || resultType == null)
        {
            String msg = "Parameter must not null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (result.getClass() == resultType)
        {
            return result;
        }
        
        if (resultType == BigDecimal.class)
        {
            if (isFloatingPointNumber(result))
            {
                return new BigDecimal(result.doubleValue());
            }
            return new BigDecimal(result.longValue());
        }
        
        if (resultType == Double.class)
        {
            return result.doubleValue();
        }
        
        if (resultType == Float.class)
        {
            return result.floatValue();
        }
        
        if (resultType == Long.class)
        {
            return result.longValue();
        }
        
        if (resultType == Integer.class)
        {
            return result.intValue();
        }
        
        LOG.error("The resultType to numberic value error.");
        throw new StreamingRuntimeException("The resultType to numberic value error.");
    }
    
    /**
     * <将Number数值转化为BigDecimal类型的值>
     * <功能详细描述>
     *
     */
    public static BigDecimal getBigDecimalValue(Number result)
    {
        if (result == null)
        {
            String msg = "Parameter must not null.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        if (result.getClass() == BigDecimal.class)
        {
            return (BigDecimal)result;
        }
        
        if (isFloatingPointNumber(result))
        {
            return new BigDecimal(result.doubleValue());
        }
        return new BigDecimal(result.longValue());
    }
    
    /**
     * <判断数值是否为浮点型>
     * <判断数值是否为浮点型>
     *
     */
    public static boolean isFloatingPointNumber(Number value)
    {
        if ((value instanceof Float) || (value instanceof Double))
        {
            return true;
        }
        return false;
    }
    
    /**
     * <返回比较类型>
     * <返回比较类型>
     *
     */
    public static Class< ? > getCompareType(Class< ? > typeOne, Class< ? > typeTwo)
        throws IllegalDataTypeException
    {
        if ((typeOne == String.class) && (typeTwo == String.class))
        {
            return String.class;
        }
        if (((typeOne == boolean.class) || ((typeOne == Boolean.class)))
            && ((typeTwo == boolean.class) || ((typeTwo == Boolean.class))))
        {
            return Boolean.class;
        }
        
        if (typeOne == null)
        {
            return typeTwo;
        }
        if (typeTwo == null)
        {
            return typeOne;
        }
        
        if (!isNumberic(typeOne) || !isNumberic(typeTwo))
        {
            String typeOneName = typeOne.getName();
            String typeTwoName = typeTwo.getName();
            IllegalDataTypeException exp =
                new IllegalDataTypeException(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE, typeOneName,
                    typeTwoName);
            LOG.error(ErrorCode.SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE.getFullMessage(typeOneName,typeTwoName), exp);
            throw exp;
        }
        
        return getArithmaticType(typeOne, typeTwo);
    }
    
    /**
     * <判断是否Booelan类型>
     * <功能详细描述>
     *
     */
    public static boolean isBoolean(Class< ? > type)
    {
        if ((type == Boolean.class) || (type == boolean.class))
        {
            return true;
        }
        return false;
    }

    /**
     * <判断是否Date或者Timestamp类型>
     * <功能详细描述>
     *
     */
    public static boolean isDateOrTimestamp(Class< ? > type)
    {
        //Time类型只能保证当天，会导致时间窗类型比较在到了第二天之后比较错误。
        return ((type == Date.class) || (type == Timestamp.class));
    }
    
}
