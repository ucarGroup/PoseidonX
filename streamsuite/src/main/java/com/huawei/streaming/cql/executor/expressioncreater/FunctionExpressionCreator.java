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

package com.huawei.streaming.cql.executor.expressioncreater;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.FunctionInfo;
import com.huawei.streaming.cql.executor.FunctionType;
import com.huawei.streaming.cql.executor.expressioncreater.functionValidater.FunctionValidatreSets;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.ConstExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.MethodExpression;
import com.huawei.streaming.process.agg.aggregator.AbstractAggregate;
import com.huawei.streaming.process.agg.aggregator.AggregateDistinctValue;
import com.huawei.streaming.udfs.UDF;

/**
 * udf函数实例创建
 * <p/>
 * UDF函数有两种，一种是静态方法接口，一种是实例化接口，要根据接口进行判断。
 * UDAF函数都继承自AggregateExpression接口，这个接口实现了一个默认的构造器，入参是一个返回值类型的Class。
 *
 */
public class FunctionExpressionCreator implements ExpressionCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionExpressionCreator.class);
    
    private FunctionExpressionDesc expressionDesc;
    
    private IExpression[] argsExpressions;
    
    private Map<String, String> systemConfig;
    
    /**
     * {@inheritDoc}
     *
     */
    @Override
    public IExpression createInstance(ExpressionDescribe expressionDescribe, Map<String, String> systemconfig)
        throws ExecutorException
    {
        this.systemConfig = systemconfig;
        expressionDesc = (FunctionExpressionDesc)expressionDescribe;
        argsExpressions = new IExpression[expressionDesc.getArgExpressions().size()];
        
        for (int i = 0; i < expressionDesc.getArgExpressions().size(); i++)
        {
            argsExpressions[i] =
                ExpressionCreatorFactory.createExpression(expressionDesc.getArgExpressions().get(i), systemconfig);
        }
        
        if (isUDAF())
        {
            return createUDAFExpression();
        }
        
        /*
         * 目前暂时不考虑udtf函数
         */
        return createUDFExpression();
    }
    
    /**
     * 创建UDAF函数表达式实例
     * <p/>
     * UDAF函数必须区分本地和非本地方法
     * <p/>
     * 如果是本地方法，在构造实例的时候，构造器中的数据类型依据参数类型来判断。
     * 所有的本地方法都只有一个入参，所以数据类型还是很容易判断出来的。
     * 如果不是本地方法，是用户自定义方法，构造器中的数据类型传入Null。
     * <p/>
     * 如果是count(*)里面的表达式是星号表达式，返回数据类型是空。
     *
     */
    private IExpression createUDAFExpression()
        throws ExecutorException
    {
        Class< ? > udafReturnType = null;
        
        /**
         * 对于count(*) 这样的，替换为count(1)
         */
        if (expressionDesc.isSelectStar())
        {
            udafReturnType = Integer.class;
        }
        
        /*
         * 只有系统的UDAF函数需要显示指定返回值
         * 如果是用户自定义的，就不需要了，由用户自己指定 
         */
        if (isNative() && argsExpressions.length != 0)
        {
            udafReturnType = argsExpressions[0].getType();
        }
        
        AbstractAggregate udaf = null;
        try
        {
            /*
             * aggregateFilter表达式
             * 这里构建出来的Filter表达式，要在构建aggregateService的时候用到。
             */
            if (argsExpressions.length == CQLConst.I_2)
            {
                Constructor< ? > constructor = expressionDesc.getFinfo().getFilterClazz().getConstructor(Class.class);
                udaf = (AbstractAggregate)constructor.newInstance(udafReturnType);
            }
            else
            {
                Constructor< ? > constructor = expressionDesc.getFinfo().getClazz().getConstructor(Class.class);
                udaf = (AbstractAggregate)constructor.newInstance(udafReturnType);
            }
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.FUNCTION_UNSPPORTED, expressionDesc.getFinfo().getName());
            LOG.error("Unsupport function.", exception);
            throw exception;
        }
        
        /**
         * 如果包含了groupby，那么就要new AggregateGroupedExpression
         * 这里没办法判断是否包含了groupby，
         * 所以，只能够先创建AggregateExpression，
         * 再在AggregateServiceViewCreator中将其转为AggregateGroupedExpression
         */
        return createAggregateExpression(udaf);
    }

    private AggregateExpression createAggregateExpression(AbstractAggregate udaf)
    {
        AggregateExpression result = new AggregateExpression(udaf, false);
        if (expressionDesc.isDistinct())
        {
            result = new AggregateExpression(new AggregateDistinctValue(udaf), true);
        }

        if (expressionDesc.isSelectStar())
        {
            result.setAggArgExpression(new ConstExpression(1L));
        }
        else
        {
            result.setAggArgExpression(argsExpressions[0]);
            if (argsExpressions.length == CQLConst.I_2)
            {
                result.setAggArgFilterExpression(argsExpressions[1]);
            }
        }
        return result;
    }

    /**
     * 创建udf表达式
     *
     */
    private IExpression createUDFExpression()
        throws ExecutorException
    {
        if (isExtendsUDF())
        {
            StreamingConfig config = new StreamingConfig();
            config.putAll(systemConfig);
            
            FunctionInfo functionInfo = expressionDesc.getFinfo();

            if (functionInfo.getProperteis() != null)
            {
                config.putAll(functionInfo.getProperteis());
            }
            
            return createInstanceOfUDFExpression(config);
        }
        
        return createStaticUDFExpression();
    }
    
    /**
     * 创建udf函数实例，这些udf函数一定都是static的方法
     *
     */
    private IExpression createStaticUDFExpression()
    {
        Class< ? > functionClazz = expressionDesc.getFinfo().getClazz();
        String methodName = expressionDesc.getFinfo().getMethodName();
        return new MethodExpression(functionClazz, methodName, argsExpressions);
    }
    
    /**
     * 创建udf函数实例，这些udf函数一定继承自UDF类
     *
     */
    private IExpression createInstanceOfUDFExpression(final StreamingConfig config)
        throws ExecutorException
    {
        FunctionInfo functionInfo = expressionDesc.getFinfo();

        /**
         * 检查函数的输入参数，防止输入非法参数
         */
        boolean validateResults = new FunctionValidatreSets().validate(functionInfo.getName(), argsExpressions, expressionDesc);
        if(!validateResults){
            ExecutorException exception =
                new ExecutorException(ErrorCode.FUNCTION_UNSUPPORTED_PARAMETERS, functionInfo.getName());
            LOG.error("Unsupport function arguments.", exception);
            throw exception;
        }
        
        Object udf = null;
        try
        {
            Constructor< ? > constructor = functionInfo.getClazz().getConstructor(Map.class);
            udf = constructor.newInstance(config);
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.FUNCTION_UNSPPORTED, functionInfo.getName());
            LOG.error("Unsupport function.", exception);
            throw exception;
        }
        return new MethodExpression(udf, functionInfo.getMethodName(), argsExpressions);
    }
    
    /**
     * 检查是否是udaf函数
     *
     */
    private boolean isUDAF()
    {
        return expressionDesc.getFinfo().getType() == FunctionType.UDAF;
    }
    
    /**
     * 是否是本地方法
     *
     */
    private boolean isNative()
    {
        return expressionDesc.getFinfo().isNative();
    }
    
    /**
     * 是否是本地方法
     *
     */
    private boolean isExtendsUDF()
    {
        Class< ? > functionClazz = expressionDesc.getFinfo().getClazz();
        return UDF.class == functionClazz.getSuperclass();
    }
    
}
