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

package com.huawei.streaming.cql.executor.pyhsicplanvalidater;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 验证连线，检查是否有重复的连线
 *
 */
public class TransitionRepeatValidater implements Validater
{
    private static final Logger LOG = LoggerFactory.getLogger(TransitionRepeatValidater.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validate(Application app)
        throws ExecutorException
    {
        checkRepeatTransition(app.getOpTransition());
    }
    
    /**
     * 检查重复的连线
     * <p/>
     * 系统要求，一个算子，接收的流名称不能重复
     *
     */
    private void checkRepeatTransition(List<OperatorTransition> transitions)
        throws ExecutorException
    {
        if (transitions == null)
        {
            return;
        }
        /*
         * Map<To算子名称，算子输入流名称数组>
         */
        Map<String, Set<String>> map = Maps.newHashMap();
        for (OperatorTransition transition : transitions)
        {
            if (!map.containsKey(transition.getToOperatorId()))
            {
                map.put(transition.getToOperatorId(), new HashSet<String>());
            }
            
            if (map.get(transition.getToOperatorId()).contains(transition.getStreamName()))
            {
                ExecutorException exception = new ExecutorException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
                LOG.error("Invalid topology define, stream not match operator.", exception);
                throw exception;
            }
            map.get(transition.getToOperatorId()).add(transition.getStreamName());
        }
        
    }
}
