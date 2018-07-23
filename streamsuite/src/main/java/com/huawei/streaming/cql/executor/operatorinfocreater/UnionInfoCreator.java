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

package com.huawei.streaming.cql.executor.operatorinfocreater;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.opereators.UnionOperator;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.operatorviewscreater.SelectViewExpressionCreator;
import com.huawei.streaming.event.EventTypeMng;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.operator.AbsOperator;
import com.huawei.streaming.operator.functionstream.UnionFunctionOp;

/**
 * 创建功能性算子实例
 * 
 */
public class UnionInfoCreator implements OperatorInfoCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(UnionInfoCreator.class);
    
    private UnionOperator functorOperator;
    
    private List<OperatorTransition> transitionIn = null;
    
    private Map<String, String> applicationConfig;
    
    private Application app;
    
    private EventTypeMng streamSchema;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AbsOperator createInstance(Application vapp, Operator operator, EventTypeMng streamschema,
        Map<String, String> systemConfig)
        throws StreamingException
    {
        this.app = vapp;
        this.streamSchema = streamschema;
        this.applicationConfig = systemConfig;
        this.functorOperator = (UnionOperator)operator;
        this.transitionIn = OperatorInfoCreatorFactory.getTransitionIn(vapp, operator);
        
        return createFunctionInfo(streamschema);
    }
    
    /**
     * 创建算子信息
     */
    protected AbsOperator createFunctionInfo(EventTypeMng streamschema)
        throws StreamingException
    {
        StreamingConfig config = new StreamingConfig();
        if (functorOperator.getArgs() != null)
        {
            config.putAll(functorOperator.getArgs());
        }
        
        UnionFunctionOp union = new UnionFunctionOp();
        config.putAll(applicationConfig);
        config.putAll(createInputExprsConf());
        union.setConfig(config);
        return OperatorInfoCreatorFactory.buildStreamOperator(functorOperator, union);
    }
    
    /**
     * 获取输入表达式 参数
     */
    public StreamingConfig createInputExprsConf()
        throws ExecutorException
    {
        StreamingConfig unionConf = new StreamingConfig();
        
        Map<String, IExpression[]> inExp = new HashMap<String, IExpression[]>();
        Map<String, IEventType> inSche = new HashMap<String, IEventType>();
        for (int i = 0; i < transitionIn.size(); i++)
        {
            Schema schema = OperatorInfoCreatorFactory.getClonedSchemaByName(transitionIn.get(i).getSchemaName(), app);
            IExpression[] exps = getInPutExpressions(schema, getSelectExprFromSchema(schema));
            inExp.put(transitionIn.get(i).getStreamName(), exps);
            inSche.put(transitionIn.get(i).getStreamName(), streamSchema.getEventType(schema.getId()));
        }
        
        unionConf.put(StreamingConfig.OPERATOR_UNION_INNER_INPUTNAMES_AND_EXPRESSION, inExp);
        unionConf.put(StreamingConfig.OPERATOR_UNION_INPUTNAMES_AND_SCHEMA, inSche);
        return unionConf;
    }
    
    /**
     * 获取表达式 解析结果
     */
    private IExpression[] getInPutExpressions(Schema scheam, String selectExpr)
        throws ExecutorException
    {
        return new SelectViewExpressionCreator().create(Arrays.asList(scheam), selectExpr, applicationConfig);
    }
    
    /**
     * 将schema中的列展开，构成简单的select表达式
     */
    private String getSelectExprFromSchema(Schema scheam)
    {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < scheam.getCols().size(); i++)
        {
            sb.append(scheam.getCols().get(i).getName() + ",");
        }
        return sb.substring(0, sb.length() - 1);
    }
    
}
