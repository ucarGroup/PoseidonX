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
package com.huawei.streaming.cql.executor;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.udfs.ToBoolean;
import com.huawei.streaming.udfs.ToDate;
import com.huawei.streaming.udfs.ToDecimal;
import com.huawei.streaming.udfs.ToDouble;
import com.huawei.streaming.udfs.ToFloat;
import com.huawei.streaming.udfs.ToInt;
import com.huawei.streaming.udfs.ToLong;
import com.huawei.streaming.udfs.ToString;
import com.huawei.streaming.udfs.ToTime;
import com.huawei.streaming.udfs.ToTimeStamp;
import com.huawei.streaming.udfs.UDF;
import com.huawei.streaming.udfs.UDFAnnotation;

/**
 * 函数注册类
 *
 * 以前这个类主要是由静态方法构成了，现在改成了对象的模式
 * 主要是为了避免多客户端并发的场景下，
 * 多用户注册同名函数所造成的冲突，
 * 该实例对象会在DriverContext中保存
 *
 */
public class FunctionRegistry
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionRegistry.class);
    
    private Map<String, FunctionInfo> functions = Maps.newHashMap();
    
    /**
     * 默认构造器
     */
    public FunctionRegistry()
    {
        functions.putAll(NativeFunctionRegistry.getNativeFunctions());
    }
    
    /**
     * 注册函数
     *
     * 注册的函数不能覆盖native本地函数，如果函数名称重复，会覆盖原有非native函数
     *
     * 这个方法是提供给CQL接口用的。
     *
     */
    public void registerUDF(String shortName, Class< ? > clazz, Map<String, String> functionProperties)
        throws CQLException
    {
        if (functions.containsKey(shortName))
        {
            FunctionInfo fInfo = functions.get(shortName);
            if (fInfo.isNative())
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.FUNCTION_OVERWRITE_NATIVE, shortName);
                LOG.error("User define function will overwritten native function.", exception);
                throw exception;
            }
        }

        FunctionInfo functionInfo = FunctionInfo.createUDFFunctionInfo(shortName,
         clazz,
         NativeFunctionRegistry.getUdfMethodName(),
         false,
         FunctionType.UDF);
        functionInfo.setProperteis(functionProperties);
        functions.put(shortName, functionInfo);
    }
    
    /**
     * 反注册系统函数
     *
     * 这个方法是提供给CQL接口用的。
     *
     */
    public void unRegisterUDF(String shortName)
        throws CQLException
    {
        if (functions.containsKey(shortName))
        {
            FunctionInfo fInfo = functions.get(shortName);
            if (fInfo.isNative())
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.FUNCTION_REMOVE_NATIVE, shortName);
                LOG.error("Remove native function.", exception);
                
                throw exception;
            }
            else
            {
                functions.remove(shortName);
            }
        }
    }
    
    /**
     * 注册系统函数
     * 
     */
    public void registerUDAF(String shortName, Class< ? > clazz)
    {
        functions.put(shortName, FunctionInfo.createUDAFFunctionInfo(shortName, clazz, null, false, FunctionType.UDAF));
    }
    
    /**
     * 根据窗口类的全名称或者窗口函数短名称
     * 主要是IDE在用
     * 
     */
    public String getFunctionNameByClass(String clazz)
    {
        for (Entry<String, FunctionInfo> et : functions.entrySet())
        {
            if (et.getValue().getClazz().getName().equals(clazz))
            {
                return et.getKey();
            }
        }
        return null;
    }
    
    /**
     * cast函数的转换
     */
    public FunctionInfo changeCastFunctionInfo(Class< ? > type)
        throws SemanticAnalyzerException
    {
        if (String.class == type)
        {
            return getFunctionInfoByFunctionClass(ToString.class);
        }
        if (Integer.class == type)
        {
            return getFunctionInfoByFunctionClass(ToInt.class);
        }
        if (Double.class == type)
        {
            return getFunctionInfoByFunctionClass(ToDouble.class);
        }
        if (Float.class == type)
        {
            return getFunctionInfoByFunctionClass(ToFloat.class);
        }
        if (Long.class == type)
        {
            return getFunctionInfoByFunctionClass(ToLong.class);
        }
        if (Boolean.class == type)
        {
            return getFunctionInfoByFunctionClass(ToBoolean.class);
        }
        if (Date.class == type)
        {
            return getFunctionInfoByFunctionClass(ToDate.class);
        }
        if (Time.class == type)
        {
            return getFunctionInfoByFunctionClass(ToTime.class);
        }
        if (Timestamp.class == type)
        {
            return getFunctionInfoByFunctionClass(ToTimeStamp.class);
        }
        if (BigDecimal.class == type)
        {
            return getFunctionInfoByFunctionClass(ToDecimal.class);
        }

        SemanticAnalyzerException exception =
            new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNSUPPORTED_DATATYPE, type.getName());
        LOG.error("Data type {} not support in cast function.", type.getName(), exception);
        
        throw exception;
    }
    
    /**
     * 根据窗口函数类获取该窗口所在类
     * 
     */
    public FunctionInfo getFunctionInfoByFunctionClass(Class< ? extends UDF > clazz)
    {
        UDFAnnotation annotation = clazz.getAnnotation(UDFAnnotation.class);
        String functionName = annotation == null ? null : annotation.value();
        return functions.get(functionName);
    }

    /**
     * 根据窗口函数短名称获取该窗口所在类
     *
     */
    public FunctionInfo getFunctionInfoByFunctionName(String functionName)
    {
        return functions.get(functionName);
    }

}
