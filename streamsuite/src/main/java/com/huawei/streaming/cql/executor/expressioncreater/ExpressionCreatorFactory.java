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

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.OrderByClauseAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.process.sort.SortCondition;

/**
 * 创建各种表表达式实例
 *
 */
public class ExpressionCreatorFactory
{
    
    private static final Logger LOG = LoggerFactory.getLogger(ExpressionCreatorFactory.class);
    
    /**
     * 创建各类表达式实例
     *
     */
    public static IExpression createExpression(ExpressionDescribe expressDesc, Map<String, String> systemConfig)
        throws ExecutorException
    {
        Class< ? extends ExpressionCreator> creatorClass =
            AnnotationUtils.getExpressionCreatorAnnoationOverClass(expressDesc.getClass());
        if (creatorClass == null)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS);
            LOG.error("Expression class is unkown.", exception);  
            throw exception;
        }
        
        ExpressionCreator creator = createExpressionCreatorInstance(creatorClass);
        return creator.createInstance(expressDesc, systemConfig);
    }
    
    private static ExpressionCreator createExpressionCreatorInstance(Class< ? extends ExpressionCreator> creator)
        throws ExecutorException
    {
        try
        {
            return creator.newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, creator.getName());
            LOG.error("Failed to create instance.", exception);
            
            throw exception;
        }
    }
    
    /**
     * 创建order by表达式
     *
     */
    public static List<SortCondition> createOrderByExpression(OrderByClauseAnalyzeContext parseContext)
        throws ExecutorException
    {
        LOG.info("start to create order by expression");
        return new OrderByExpressionCreator().createInstance(parseContext);
    }
}
