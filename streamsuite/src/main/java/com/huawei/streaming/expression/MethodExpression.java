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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingRuntimeException;

import net.sf.cglib.reflect.FastClass;
import net.sf.cglib.reflect.FastMethod;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.udfs.UDFAnnotation;

/**
 * Java函数表达式，支持Java函数调用
 */
public class MethodExpression implements IExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = -3061097213177802641L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(MethodExpression.class);
    
    /**
     * 传入Java方法的参数，为表达式
     */
    private IExpression[] expr = null;
    
    /**
     * Java类的类名，要求为全类名（包括包名）
     */
    private Class< ? > declareClass;
    
    /**
     * 需要调用的Java方法的名字
     */
    private String methodName;
    
    /**
     * 存放找到的方法
     */
    private transient FastMethod fastMethod;
    
    /**
     * 对象实例，如果不为null，则调用该对象实例的相应方法
     */
    private Object obj;
    
    /**
     * 构造函数
     */
    public MethodExpression(Class< ? > declareClass, String methodName, IExpression[] expr)
    {
        this.declareClass = declareClass;
        this.methodName = methodName;
        this.expr = expr;
    }
    
    /**
     * 构造函数
     */
    public MethodExpression(Object object, String methodName, IExpression[] expr)
    {
        this.obj = object;
        this.methodName = methodName;
        this.expr = expr;
        this.declareClass = object.getClass();
    }
    
    /**
     * 调用Java方法求值
     */
    @Override
    public Object evaluate(IEvent theEvent)
    {
        if (null == fastMethod)
        {
            resolveMethod(expr);
        }
        
        if (null == theEvent)
        {
            throw new RuntimeException("theEvent is null.");
        }
        
        //计算表达式的值，作为函数的参数
        Object[] resultTemp = null;
        if (expr != null)
        {
            resultTemp = new Object[expr.length];
            for (int i = 0; i < expr.length; i++)
            {
                resultTemp[i] = expr[i].evaluate(theEvent);
            }
        }
        
        //通过反射调用函数获取结果
        try
        {
            return fastMethod.invoke(obj, resultTemp);
        }
        catch (InvocationTargetException e)
        {
            LOG.error("Invoke method caught exception.", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * 根据参数获取函数
     */
    private void resolveMethod(IExpression[] exprs)
    {
        Class< ? >[] paramTypes = null;
        if (exprs != null)
        {
            //获得函数参数类型
            paramTypes = new Class[exprs.length];
            for (int i = 0; i < paramTypes.length; i++)
            {
                paramTypes[i] = exprs[i].getType();
            }
        }
        
        //获得相应方法
        Method method = MethodResolver.resolveMethod(declareClass, methodName, paramTypes);
        if (null == method)
        {
            UDFAnnotation annotation = declareClass.getAnnotation(UDFAnnotation.class);
            String funcitonName = annotation == null ? declareClass.getSimpleName() : annotation.value();
            //运行时调用，和resolveType方法不同
            StreamingRuntimeException exception =
             new StreamingRuntimeException(ErrorCode.FUNCTION_UNSUPPORTED_PARAMETERS, funcitonName);
            LOG.error(ErrorCode.FUNCTION_UNSUPPORTED_PARAMETERS.getFullMessage(funcitonName));
            throw exception;
        }
        FastClass declaringClass =
            FastClass.create(Thread.currentThread().getContextClassLoader(), method.getDeclaringClass());
        fastMethod = declaringClass.getMethod(method);
    }
    
    private Class< ? > resolveType(IExpression[] exprs)
    {
        //获得函数参数类型
        Class< ? >[] paramTypes = new Class[exprs.length];
        for (int i = 0; i < paramTypes.length; i++)
        {
            paramTypes[i] = exprs[i].getType();
        }
        
        //获得相应方法
        Method method = MethodResolver.resolveMethod(declareClass, methodName, paramTypes);
        if (null == method)
        {
            UDFAnnotation annotation = declareClass.getAnnotation(UDFAnnotation.class);
            String functionName = annotation == null ? declareClass.getSimpleName() : annotation.value();
            StreamingRuntimeException exception =
             new StreamingRuntimeException(ErrorCode.FUNCTION_UNSUPPORTED_PARAMETERS, functionName);
            LOG.error(ErrorCode.FUNCTION_UNSUPPORTED_PARAMETERS.getFullMessage(functionName));
            throw exception;
        }
        FastClass declaringClass =
            FastClass.create(Thread.currentThread().getContextClassLoader(), method.getDeclaringClass());
        FastMethod md = declaringClass.getMethod(method);
        return warpDataType(md.getReturnType());
    }
    
    private Class< ? > warpDataType(Class< ? > type)
    {
        if (type == int.class)
        {
            return Integer.class;
        }
        
        if (type == double.class)
        {
            return Double.class;
        }
        
        if (type == long.class)
        {
            return Long.class;
        }
        
        if (type == float.class)
        {
            return Float.class;
        }
        
        if (type == boolean.class)
        {
            return Boolean.class;
        }
        
        return type;
    }
    
    /**
     * 根据事件列表计算函数值
     */
    public Object evaluate(IEvent[] eventsPerStream)
    {
        
        if (null == fastMethod)
        {
            resolveMethod(expr);
        }
        
        if (null == eventsPerStream)
        {
            throw new RuntimeException("theEvent is null.");
        }
        
        //计算表达式的值，作为函数的参数
        Object[] resultTemp = new Object[expr.length];
        for (int i = 0; i < expr.length; i++)
        {
            resultTemp[i] = expr[i].evaluate(eventsPerStream);
        }
        
        //通过反射调用函数获取结果
        try
        {
            return fastMethod.invoke(obj, resultTemp);
        }
        catch (InvocationTargetException e)
        {
            LOG.error("Invoke method caught exception.", e);
            throw new RuntimeException(e);
        }
        
    }
    
    @Override
    public Class< ? > getType()
    {
        return resolveType(expr);
    }
    
    public IExpression[] getExpr()
    {
        return expr;
    }
}
