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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;

import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;

/**
 * 执行器中的公共类
 * 
 */
public class ExecutorUtils
{
    /**
     * 移除字符串中的流名称
     */
    public static String removeStreamName(String str)
    {
        if (StringUtils.isEmpty(str))
        {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        String[] fields = str.split(",");
        for (int i = 0; i < fields.length; i++)
        {
            String[] streamAndType = fields[i].split("\\.");
            sb.append(streamAndType[streamAndType.length - 1]);
            if (i != fields.length - 1)
            {
                sb.append(",");
            }
        }
        return sb.toString();
    }
    
    /**
     * 根据id获取对应算子
     */
    public static Operator getOperatorById(String opid, List<Operator> operators)
    {
        if (operators == null || operators.size() == 0)
        {
            return null;
        }
        
        for (Operator op : operators)
        {
            if (op.getId().equals(opid))
            {
                return op;
            }
        }
        return null;
    }
    
    /**
     * 获取算子中最开始的连线
     * 
     * 从To的数组中找到从来没有被From使用过的点即可
     */
    public static List<OperatorTransition> getLastTransitons(List<OperatorTransition> transitions)
    {
        List<OperatorTransition> res = new ArrayList<OperatorTransition>();
        
        Set<String> froms = getFromTransitons(transitions);
        Set<String> tos = getToTransitons(transitions);
        for (String s : tos)
        {
            if (!froms.contains(s))
            {
                res.addAll(getTransitonsByToId(s, transitions));
            }
        }
        return res;
    }
    
    /**
     * 获取算子中最开始的连线
     * 
     * 一般是一条，如果是join，那么就有两条了
     * 
     * 只要找到From的点中从来没有被To使用过的点即可
     */
    public static List<OperatorTransition> getFirstTransitons(List<OperatorTransition> transitions)
    {
        List<OperatorTransition> res = new ArrayList<OperatorTransition>();
        
        Set<String> froms = getFromTransitons(transitions);
        Set<String> tos = getToTransitons(transitions);
        for (String s : froms)
        {
            if (!tos.contains(s))
            {
                res.addAll(getTransitonsByFromId(s, transitions));
            }
        }
        return res;
    }
    
    /**
     * 获取所有连向toid的连线
     */
    public static List<OperatorTransition> getTransitonsByToId(String toid, List<OperatorTransition> transitions)
    {
        List<OperatorTransition> toTransitions = new ArrayList<OperatorTransition>();
        
        for (OperatorTransition transition : transitions)
        {
            if (transition.getToOperatorId().equals(toid))
            {
                toTransitions.add(transition);
            }
        }
        return toTransitions;
    }
    
    /**
     * 获取所有从fromid发起的连线
     */
    public static List<OperatorTransition> getTransitonsByFromId(String formid, List<OperatorTransition> transitions)
    {
        List<OperatorTransition> fromTransitions = new ArrayList<OperatorTransition>();
        
        for (OperatorTransition transition : transitions)
        {
            if (transition.getFromOperatorId().equals(formid))
            {
                fromTransitions.add(transition);
            }
        }
        return fromTransitions;
    }
    
    private static Set<String> getFromTransitons(List<OperatorTransition> transitions)
    {
        Set<String> toTransitions = new HashSet<String>();
        for (OperatorTransition transition : transitions)
        {
            toTransitions.add(transition.getFromOperatorId());
        }
        return toTransitions;
    }
    
    private static Set<String> getToTransitons(List<OperatorTransition> transitions)
    {
        Set<String> toTransitions = new HashSet<String>();
        for (OperatorTransition transition : transitions)
        {
            toTransitions.add(transition.getToOperatorId());
        }
        return toTransitions;
    }
    
}
