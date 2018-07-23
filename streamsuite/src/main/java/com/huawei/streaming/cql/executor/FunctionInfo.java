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

import java.util.Map;

/**
 * 函数信息
 * 在注册函数的时候用到
 * 用于给udf函数和udaf函数对外一致的表现。
 * 
 * 由于udf函数中还包含了很多java自带的静态函数，这些都没有统一接口的，所以都要统一起来。
 * 
 */
public class FunctionInfo
{
    /**
     * 函数名称
     * 函数名称全部小写。
     */
    private String name;
    
    /**
     * 函数类型
     */
    private FunctionType type;
    
    /**
     * 函数所在类名称
     */
    private Class< ? > clazz;
    
    /**
     * 当存在过滤表达式的时候的函数所在类名称
     */
    private Class< ? > filterClazz;
    
    /**
     * 方法名称
     * 只在udf函数中用到，udaf不需要。
     * 一般也只是在静态类中用到。
     */
    private String methodName;
    
    /**
     * 是否是本地方法
     * 这个是用来识别是否是系统自带方法，
     * 如果该函数是用户自定义的，该属性值一定是false
     */
    private boolean isNative;
    /**
     * 函数的配置属性
     * 这个配置属性的存在，允许一个UDF实现被注册为多个函数
     * 多个函数之间使用不同的参数，互不影响
     */
    private Map<String,String> properteis;

    /**
     * 创建udf函数信息
     * 
     */
    public static FunctionInfo createUDFFunctionInfo(String name, Class< ? > clazz, String methodName,
        boolean isNative, FunctionType functionType)
    {
        FunctionInfo f = createFunctionInfoWithOutType(name, clazz, methodName, isNative, functionType);
        return f;
    }
    
    /**
     * 创建udaf函数信息
     * 
     */
    public static FunctionInfo createUDAFFunctionInfo(String name, Class< ? > clazz, Class< ? > filterClazz,
        boolean isNative, FunctionType functionType)
    {
        FunctionInfo f = createFunctionInfoWithOutType(name, clazz, null, isNative, functionType);
        f.setFilterClazz(filterClazz);
        return f;
    }
    
    /**
     * 创建udtf函数信息
     * 
     */
    public static FunctionInfo createUDTFFunctionInfo(String name, Class< ? > clazz, String methodName,
        boolean isNative, FunctionType functionType)
    {
        FunctionInfo f = createFunctionInfoWithOutType(name, clazz, methodName, isNative, functionType);
        return f;
    }
    
    /**
     * 创建udf函数信息
     * 
     */
    private static FunctionInfo createFunctionInfoWithOutType(String name, Class< ? > clazz, String methodName,
        boolean isNative, FunctionType functionType)
    {
        FunctionInfo f = new FunctionInfo();
        f.setName(name);
        f.setClazz(clazz);
        f.setMethodName(methodName);
        f.setNative(isNative);
        f.setType(functionType);
        return f;
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public FunctionType getType()
    {
        return type;
    }
    
    public void setType(FunctionType type)
    {
        this.type = type;
    }
    
    public Class< ? > getClazz()
    {
        return clazz;
    }
    
    public void setClazz(Class< ? > clazz)
    {
        this.clazz = clazz;
    }
    
    public String getMethodName()
    {
        return methodName;
    }
    
    public void setMethodName(String methodName)
    {
        this.methodName = methodName;
    }
    
    public boolean isNative()
    {
        return isNative;
    }
    
    public void setNative(boolean isnative)
    {
        this.isNative = isnative;
    }
    
    public Class< ? > getFilterClazz()
    {
        return filterClazz;
    }
    
    public void setFilterClazz(Class< ? > filterClazz)
    {
        this.filterClazz = filterClazz;
    }

    public Map< String, String > getProperteis()
    {
        return properteis;
    }

    public void setProperteis(Map< String, String > properteis)
    {
        this.properteis = properteis;
    }

}
