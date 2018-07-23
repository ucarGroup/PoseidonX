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

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.cql.executor.ExecutorUtils;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;

/**
 * 各类语义分析结果拆分内容
 * 
 */
public class SplitContext
{
    private List<OperatorTransition> transitions = new ArrayList<OperatorTransition>();
    
    private List<Operator> operators = new ArrayList<Operator>();
    
    /**
     * 输出的流名称
     * 指的是在CQL中显示指定输出流名称的。
     * 例如insert into 之类的语句
     */
    private String outputStreamName;
    
    /**
     * CQL解析结果
     * 通过这个结果，可以进行多个CQL之间的连接
     */
    private AnalyzeContext parseContext;
    
    /**
     * 添加连线
     */
    public void addTransitions(OperatorTransition transition)
    {
        transitions.add(transition);
    }
    
    /**
     * 通过id获取算子
     */
    public Operator getOperatorById(String opid)
    {
        return ExecutorUtils.getOperatorById(opid, operators);
    }
    
    /**
     * 获取算子中最开始的连线
     * 
     * 从To的数组中找到从来没有被From使用过的点即可
     * 
     */
    public List<OperatorTransition> getLastTransitons()
    {
        return ExecutorUtils.getLastTransitons(transitions);
    }
    
    /**
     * 获取算子中最开始的连线
     * 
     * 一般是一条，如果是join，那么就有两条了
     * 
     * 只要找到From的点中从来没有被To使用过的点即可
     * 
     */
    public List<OperatorTransition> getFirstTransitons()
    {
        return ExecutorUtils.getFirstTransitons(transitions);
    }
    
    /**
     * 添加算子
     */
    public void addOperators(Operator op)
    {
        operators.add(op);
    }
    
    /**
     * 获取开始的第一个算子
     */
    public Operator getFirstOperator()
    {
        if (operators == null || operators.size() == 0)
        {
            return null;
        }
        return operators.get(0);
    }
    
    public Operator getLastOperator()
    {
        return operators.get(operators.size() - 1);
    }
    
    public List<OperatorTransition> getTransitions()
    {
        return transitions;
    }
    
    public List<Operator> getOperators()
    {
        return operators;
    }
    
    public String getOutputStreamName()
    {
        return outputStreamName;
    }
    
    public void setOutputStreamName(String outputStreamName)
    {
        this.outputStreamName = outputStreamName;
    }
    
    public AnalyzeContext getParseContext()
    {
        return parseContext;
    }
    
    public void setParseContext(AnalyzeContext parseContext)
    {
        this.parseContext = parseContext;
    }
}
