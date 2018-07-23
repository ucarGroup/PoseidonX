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

package com.huawei.streaming.expression;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 调用类的静态方法
 * 如果方法参数不完全匹配，则使用java的自动类型转换进行匹配，找到需要转换最少个数参数的方法
 */
public class MethodResolver
{
    /**
     * 存放基本类型和它的包装类型
     */
    private static final Map<Class< ? >, Set<Class< ? >>> WRAPPING_CONVERSIONS =
        new HashMap<Class< ? >, Set<Class< ? >>>();
    
    /**
     * 存放基本类型和它支持的转换类型
     */
    private static final Map<Class< ? >, Set<Class< ? >>> WIDENING_CONVERSIONS =
        new HashMap<Class< ? >, Set<Class< ? >>>();
    
    /**
     * 存放基本类型的顺序的权重，为了找到最合适的方法，如byte类型会去匹配int而不匹配long（假如没有byte类型的函数）
     */
    private static final Map<Class< ? >, Integer> TYPE_SEQUENCE = new HashMap<Class< ? >, Integer>();
    
    private static final int SHORT_SEQUENCE = 2;
    
    private static final int CHAR_SEQUENCE = 2;
    
    private static final int INT_SEQUENCE = 3;
    
    private static final int LONG_SEQUENCE = 4;
    
    private static final int FLOAT_SEQUENCE = 5;
    
    private static final int DOUBLE_SEQUENCE = 6;
    
    private static final int BASE = 10;
    
    static
    {
        //初始化基本类型的权重次序
        TYPE_SEQUENCE.put(byte.class, 1);
        TYPE_SEQUENCE.put(Byte.class, 1);
        TYPE_SEQUENCE.put(short.class, SHORT_SEQUENCE);
        TYPE_SEQUENCE.put(Short.class, SHORT_SEQUENCE);
        TYPE_SEQUENCE.put(char.class, CHAR_SEQUENCE);
        TYPE_SEQUENCE.put(Character.class, CHAR_SEQUENCE);
        TYPE_SEQUENCE.put(int.class, INT_SEQUENCE);
        TYPE_SEQUENCE.put(Integer.class, INT_SEQUENCE);
        TYPE_SEQUENCE.put(long.class, LONG_SEQUENCE);
        TYPE_SEQUENCE.put(Long.class, LONG_SEQUENCE);
        TYPE_SEQUENCE.put(float.class, FLOAT_SEQUENCE);
        TYPE_SEQUENCE.put(Float.class, FLOAT_SEQUENCE);
        TYPE_SEQUENCE.put(double.class, DOUBLE_SEQUENCE);
        TYPE_SEQUENCE.put(Double.class, DOUBLE_SEQUENCE);
        
        //添加boolean类型
        Set<Class< ? >> booleanWrappers = new HashSet<Class< ? >>();
        booleanWrappers.add(boolean.class);
        booleanWrappers.add(Boolean.class);
        WRAPPING_CONVERSIONS.put(boolean.class, booleanWrappers);
        WRAPPING_CONVERSIONS.put(Boolean.class, booleanWrappers);
        
        //添加char类型
        Set<Class< ? >> charWrappers = new HashSet<Class< ? >>();
        charWrappers.add(char.class);
        charWrappers.add(Character.class);
        WRAPPING_CONVERSIONS.put(char.class, charWrappers);
        WRAPPING_CONVERSIONS.put(Character.class, charWrappers);
        
        //添加byte类型
        Set<Class< ? >> byteWrappers = new HashSet<Class< ? >>();
        byteWrappers.add(byte.class);
        byteWrappers.add(Byte.class);
        WRAPPING_CONVERSIONS.put(byte.class, byteWrappers);
        WRAPPING_CONVERSIONS.put(Byte.class, byteWrappers);
        
        //添加short类型
        Set<Class< ? >> shortWrappers = new HashSet<Class< ? >>();
        shortWrappers.add(short.class);
        shortWrappers.add(Short.class);
        WRAPPING_CONVERSIONS.put(short.class, shortWrappers);
        WRAPPING_CONVERSIONS.put(Short.class, shortWrappers);
        
        //添加int类型
        Set<Class< ? >> intWrappers = new HashSet<Class< ? >>();
        intWrappers.add(int.class);
        intWrappers.add(Integer.class);
        WRAPPING_CONVERSIONS.put(int.class, intWrappers);
        WRAPPING_CONVERSIONS.put(Integer.class, intWrappers);
        
        //添加long类型
        Set<Class< ? >> longWrappers = new HashSet<Class< ? >>();
        longWrappers.add(long.class);
        longWrappers.add(Long.class);
        WRAPPING_CONVERSIONS.put(long.class, longWrappers);
        WRAPPING_CONVERSIONS.put(Long.class, longWrappers);
        
        //添加float类型
        Set<Class< ? >> floatWrappers = new HashSet<Class< ? >>();
        floatWrappers.add(float.class);
        floatWrappers.add(Float.class);
        WRAPPING_CONVERSIONS.put(float.class, floatWrappers);
        WRAPPING_CONVERSIONS.put(Float.class, floatWrappers);
        
        //添加double类型
        Set<Class< ? >> doubleWrappers = new HashSet<Class< ? >>();
        doubleWrappers.add(double.class);
        doubleWrappers.add(Double.class);
        WRAPPING_CONVERSIONS.put(double.class, doubleWrappers);
        WRAPPING_CONVERSIONS.put(Double.class, doubleWrappers);
        
        //添加short类型支持的转换类型
        Set<Class< ? >> wideningConversionsSet = new HashSet<Class< ? >>(byteWrappers);
        WIDENING_CONVERSIONS.put(short.class, new HashSet<Class< ? >>(wideningConversionsSet));
        
        //添加int类型支持的转换类型
        wideningConversionsSet.addAll(shortWrappers);
        wideningConversionsSet.addAll(charWrappers);
        WIDENING_CONVERSIONS.put(int.class, new HashSet<Class< ? >>(wideningConversionsSet));
        
        //添加long类型支持的转换类型
        wideningConversionsSet.addAll(intWrappers);
        WIDENING_CONVERSIONS.put(long.class, new HashSet<Class< ? >>(wideningConversionsSet));
        
        //添加float类型支持的转换类型
        wideningConversionsSet.addAll(longWrappers);
        WIDENING_CONVERSIONS.put(float.class, new HashSet<Class< ? >>(wideningConversionsSet));
        
        //添加double类型支持的转换类型
        wideningConversionsSet.addAll(floatWrappers);
        WIDENING_CONVERSIONS.put(double.class, new HashSet<Class< ? >>(wideningConversionsSet));
    }
    
    /**
     * 根据类、方法名、参数类型匹配最合适的方法
     */
    public static Method resolveMethod(Class< ? > declareClass, String methodName, Class< ? >[] paramTypes)
    {
        Method[] declaredMethods = declareClass.getDeclaredMethods();
        
        Method bestMatchMethod = null;
        
        int bestConversionCount = -1;
        
        for (Method method : declaredMethods)
        {
            //仅支持public和static类型的函数
            if (!isPublic(method))
            {
                continue;
            }
            
            //判断函数名是否一致
            if (!method.getName().equals(methodName))
            {
                continue;
            }
            
            //获得参数列表的匹配度
            int conversionCount = compareParamTypes(method.getParameterTypes(), paramTypes);
            
            //该方法匹配失败
            if (conversionCount == -1)
            {
                continue;
            }
            
            //完全匹配，不需再找
            if (conversionCount == 0)
            {
                bestMatchMethod = method;
                break;
            }
            
            //更新最匹配的方法
            if (null == bestMatchMethod)
            {
                bestMatchMethod = method;
                bestConversionCount = conversionCount;
            }
            else
            {
                if (conversionCount < bestConversionCount)
                {
                    bestMatchMethod = method;
                    bestConversionCount = conversionCount;
                }
            }
        }
        
        return bestMatchMethod;
    }
    
    private static boolean isPublic(Method method)
    {
        int modifiers = method.getModifiers();
        return Modifier.isPublic(modifiers);
    }
    
    /**
     * 获得函数参数类型的匹配度，以需要转换的参数个数来衡量
     */
    private static int compareParamTypes(Class< ? >[] declareParamTypes, Class< ? >[] realParamTypes)
    {
        if (null == realParamTypes || realParamTypes.length == 0)
        {
            return declareParamTypes.length == 0 ? 0 : -1;
        }
        
        if (declareParamTypes.length != realParamTypes.length)
        {
            return -1;
        }
        
        int conversionCount = 0;
        
        for (int i = 0; i < declareParamTypes.length; i++)
        {
            //null和非基本类型匹配
            if ((realParamTypes[i] == null) && !(declareParamTypes[i].isPrimitive()))
            {
                continue;
            }
            
            if (!isIdentityConversion(declareParamTypes[i], realParamTypes[i]))
            {
                if (!isWideningConversion(declareParamTypes[i], realParamTypes[i]))
                {
                    conversionCount = -1;
                    break;
                }
                conversionCount = conversionCount + BASE + TYPE_SEQUENCE.get(declareParamTypes[i]);
            }
        }
        return conversionCount;
    }
    
    /**
     * 判断两个类型是否绝对匹配，绝对匹配指类型一样、父子类关系、包装类关系
     */
    private static boolean isIdentityConversion(Class< ? > declareParamType, Class< ? > realParamType)
    {
        if (WRAPPING_CONVERSIONS.containsKey(declareParamType))
        {
            return WRAPPING_CONVERSIONS.get(declareParamType).contains(realParamType)
                || declareParamType.isAssignableFrom(realParamType);
        }
        else
        {
            if (realParamType == null)
            {
                return !declareParamType.isPrimitive();
            }
            return declareParamType.isAssignableFrom(realParamType);
        }
    }
    
    /**
     * 
     * 判断是否可以转型，按照java语言的默认规则
     */
    private static boolean isWideningConversion(Class< ? > declareParamType, Class< ? > realParamType)
    {
        return WIDENING_CONVERSIONS.containsKey(declareParamType)
            && WIDENING_CONVERSIONS.get(declareParamType).contains(realParamType);
    }
    
}
