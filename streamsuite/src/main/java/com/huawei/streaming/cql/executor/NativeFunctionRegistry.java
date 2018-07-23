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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.process.agg.aggregator.avg.AggregateAvg;
import com.huawei.streaming.process.agg.aggregator.avg.AggregateAvgFilter;
import com.huawei.streaming.process.agg.aggregator.count.AggregateCount;
import com.huawei.streaming.process.agg.aggregator.count.AggregateCountFilter;
import com.huawei.streaming.process.agg.aggregator.max.AggregateMax;
import com.huawei.streaming.process.agg.aggregator.max.AggregateMaxFilter;
import com.huawei.streaming.process.agg.aggregator.min.AggregateMin;
import com.huawei.streaming.process.agg.aggregator.min.AggregateMinFilter;
import com.huawei.streaming.process.agg.aggregator.sum.AggregateSum;
import com.huawei.streaming.process.agg.aggregator.sum.AggregateSumFilter;
import com.huawei.streaming.udfs.Day;
import com.huawei.streaming.udfs.StringConcat;
import com.huawei.streaming.udfs.DateDiff;
import com.huawei.streaming.udfs.DayofMonth;
import com.huawei.streaming.udfs.FromUnixTime;
import com.huawei.streaming.udfs.Hour;
import com.huawei.streaming.udfs.StringToLower;
import com.huawei.streaming.udfs.Minute;
import com.huawei.streaming.udfs.Month;
import com.huawei.streaming.udfs.Second;
import com.huawei.streaming.udfs.UDF;
import com.huawei.streaming.udfs.Abs;
import com.huawei.streaming.udfs.UDFAnnotation;
import com.huawei.streaming.udfs.CurrentTimeMillis;
import com.huawei.streaming.udfs.DateAdd;
import com.huawei.streaming.udfs.DateSub;
import com.huawei.streaming.udfs.StringLength;
import com.huawei.streaming.udfs.SubString;
import com.huawei.streaming.udfs.ToDecimal;
import com.huawei.streaming.udfs.ToBoolean;
import com.huawei.streaming.udfs.ToDate;
import com.huawei.streaming.udfs.ToDouble;
import com.huawei.streaming.udfs.ToFloat;
import com.huawei.streaming.udfs.ToInt;
import com.huawei.streaming.udfs.ToLong;
import com.huawei.streaming.udfs.ToString;
import com.huawei.streaming.udfs.ToTime;
import com.huawei.streaming.udfs.ToTimeStamp;
import com.huawei.streaming.udfs.StringTrim;
import com.huawei.streaming.udfs.StringToUpper;
import com.huawei.streaming.udfs.WeekOfYear;
import com.huawei.streaming.udfs.Year;

/**
 * 本地函数注册
 * <p/>
 * 该类中的所有函数均以静态的形式初始化
 * 保证只被初始化一次
 * <p/>
 * 这主要是为了避免多客户端同时使用场景下的函数冲突
 *
 */
public class NativeFunctionRegistry
{
    
    private static final Logger LOG = LoggerFactory.getLogger(NativeFunctionRegistry.class);
    
    private static final String UDF_METHOD_NAME = "evaluate";
    
    private static final Map<String, FunctionInfo> NATIVE_FUNCTIONS =
        Collections.synchronizedMap(new LinkedHashMap<String, FunctionInfo>());
    
    static
    {
        /*
         * 类型转换函数
         * 类型转换函数利用了对象中的valueof函数，
         * 所以这就要求，每个数据类型都必须实现valueOf接口
         * 这也是语义分析的时候，常量数据类型解析的需求。
         */
        registerNativeUDF(ToString.class);
        registerNativeUDF(ToInt.class);
        registerNativeUDF(ToLong.class);
        registerNativeUDF(ToFloat.class);
        registerNativeUDF(ToDouble.class);
        registerNativeUDF(ToBoolean.class);
        registerNativeUDF(ToDate.class);
        registerNativeUDF(ToTime.class);
        registerNativeUDF(ToTimeStamp.class);
        registerNativeUDF(ToDecimal.class);
        
        /*
         * 其他功能类函数
         */
        registerNativeUDF(SubString.class);
        registerNativeUDF(StringLength.class);
        registerNativeUDF(StringTrim.class);
        registerNativeUDF(StringConcat.class);
        registerNativeUDF(StringToUpper.class);
        registerNativeUDF(StringToLower.class);
        
        /*
         * 数学函数
         */
        registerNativeUDF(Abs.class);

        /*
         * 时间类函数
         */
        registerNativeUDF(Day.class);
        registerNativeUDF(DayofMonth.class);
        registerNativeUDF(Month.class);
        registerNativeUDF(Year.class);
        registerNativeUDF(Hour.class);
        registerNativeUDF(Minute.class);
        registerNativeUDF(Second.class);
        registerNativeUDF(FromUnixTime.class);
        registerNativeUDF(WeekOfYear.class);
        registerNativeUDF(DateAdd.class);
        registerNativeUDF(DateSub.class);
        registerNativeUDF(DateDiff.class);
        registerNativeUDF(CurrentTimeMillis.class);
        
        registerNativeUDAF("avg", AggregateAvg.class, AggregateAvgFilter.class);
        registerNativeUDAF("count", AggregateCount.class, AggregateCountFilter.class);
        registerNativeUDAF("max", AggregateMax.class, AggregateMaxFilter.class);
        registerNativeUDAF("min", AggregateMin.class, AggregateMinFilter.class);
        registerNativeUDAF("sum", AggregateSum.class, AggregateSumFilter.class);
        
        /*
        * 表达式
        * 函数中并不会直接使用
        * 仅仅起到占位的作用
        */
        registerNativeUDF("cast", ToString.class);
        registerNativeUDF("case", Boolean.class, "case");
        registerNativeUDF("when", Boolean.class, "when");
        registerNativeUDF("previous", Boolean.class, "previous");
        registerNativeUDF("in", Boolean.class, "in");
        registerNativeUDF("like", Boolean.class, "like");
        registerNativeUDF("between", Boolean.class, "between");
    }
    
    public static String getUdfMethodName()
    {
        return UDF_METHOD_NAME;
    }
    
    public static Map<String, FunctionInfo> getNativeFunctions()
    {
        return NATIVE_FUNCTIONS;
    }
    
    /**
     * 注册静态系统函数
     * <p/>
     * 这个方法是提供给CQL接口用的。
     *
     */
    public static void registerNativeStaticUDF(String shortName, Class< ? > clazz, String methodName)
    {
        NATIVE_FUNCTIONS.put(shortName,
            FunctionInfo.createUDFFunctionInfo(shortName, clazz, methodName, false, FunctionType.UDF));
    }
    
    /**
     * 注册本地的系统函数
     *
     */
    private static void registerNativeUDF(Class< ? extends UDF> clazz)
    {
        UDFAnnotation annotation = clazz.getAnnotation(UDFAnnotation.class);
        String funcitonName = annotation == null ? null : annotation.value();
        if (funcitonName == null)
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.FUNCTION_UNSPPORTED, "<NULL>");
            LOG.error("Unsupport function.", exception);
        }
        
        registerNativeUDF(funcitonName, clazz);
    }

    /**
     * 注册本地的系统函数
     *
     */
    private static void registerNativeUDF(String shortName, Class< ? > clazz)
    {
        if (isExtendsUDF(clazz))
        {
            NATIVE_FUNCTIONS.put(shortName,
                FunctionInfo.createUDFFunctionInfo(shortName, clazz, UDF_METHOD_NAME, true, FunctionType.UDF));
        }
        else
        {
            SemanticAnalyzerException exception =
                new SemanticAnalyzerException(ErrorCode.FUNCTION_ERROR_EXTENDS, clazz.getName());
            LOG.error("User defined function {} not extend from UDF class.", clazz.getName(), exception);
            
        }
    }
    
    /**
     * 是否是本地方法
     *
     */
    public static boolean isExtendsUDF(Class< ? > functionClass)
    {
        Class< ? > superClass = functionClass.getSuperclass();
        while (UDF.class != superClass)
        {
            superClass = superClass.getSuperclass();
            if (superClass == Object.class)
            {
                return false;
            }
        }
        
        return true;
    }
    
    /**
     * 注册系统函数
     *
     */
    private static void registerNativeUDAF(String shortName, Class< ? > clazz, Class< ? > filterClazz)
    {
        NATIVE_FUNCTIONS.put(shortName,
            FunctionInfo.createUDAFFunctionInfo(shortName, clazz, filterClazz, true, FunctionType.UDAF));
    }
    
    /**
     * 注册系统函数
     *
     */
    private static void registerNativeUDF(String shortName, Class< ? > clazz, String methodName)
    {
        NATIVE_FUNCTIONS.put(shortName,
            FunctionInfo.createUDFFunctionInfo(shortName, clazz, methodName, true, FunctionType.UDF));
    }
    
}
