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

package com.huawei.streaming.cql.builder.operatorsplitter;

import java.lang.reflect.Constructor;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 算子拆分类创建工厂方法
 *
 */
public class OperatorSplitter
{
    private static final Logger LOG = LoggerFactory.getLogger(OperatorSplitter.class);
    
    private static List<Splitter> splitters = Lists.newArrayList();
    
    static
    {
        //这里的splitter实例仅用来执行validate方法，真正算子的创建，则是通过反射创建的实例
        //所以不用担心builderUtils参数为空
        splitters.add(new SourceOperatorSplitter(null));
        splitters.add(new InsertSplitter(null));
        splitters.add(new JoinSplitter(null));
        splitters.add(new AggregateSplitter(null));
        splitters.add(new CombineSplitter(null));
        splitters.add(new DataSourceSplitter(null));
        splitters.add(new MultiInsertSplitter(null));
        splitters.add(new UserOperatorSplitter(null));
    }
    
    /**
     * 创建splitter的实例并直接split
     *
     */
    public static SplitContext split(BuilderUtils buildUtils, AnalyzeContext parseContext)
        throws ApplicationBuildException
    {
        for (Splitter splitter : splitters)
        {
            if (splitter.validate(parseContext))
            {
                return createSplitter(splitter.getClass(), buildUtils).split(parseContext);
            }
        }
        
        return null;
    }
    
    private static Splitter createSplitter(Class< ? extends Splitter> splitterClass, BuilderUtils buildUtils)
        throws ApplicationBuildException
    {
        try
        {
            Constructor< ? > constructor = splitterClass.getConstructor(BuilderUtils.class);
            return (Splitter)constructor.newInstance(buildUtils);
        }
        catch (ReflectiveOperationException e)
        {
            ApplicationBuildException exception =
                new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, splitterClass.getName());
            LOG.error("Failed to create splitter instance.", exception);
            throw exception;
        }
        
    }
    
}
