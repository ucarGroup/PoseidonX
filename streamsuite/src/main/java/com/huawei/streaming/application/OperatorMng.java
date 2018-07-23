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

package com.huawei.streaming.application;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.huawei.streaming.event.IEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.operator.OutputOperator;
import com.huawei.streaming.operator.functionstream.SplitOp;

/**
 * 应用中的算子管理
 * <功能详细描述>
 *
 */
public class OperatorMng
{
    private static final Logger LOG = LoggerFactory.getLogger(OperatorMng.class);
    
    //DFG排序后的功能算子列表，作为创建Storm拓扑顺序的基础
    private List<IRichOperator> sortedFunctions;
    
    private Map<String, IRichOperator> inputs;
    
    private Map<String, IRichOperator> functions;
    
    private Map<String, IRichOperator> outputs;
    
    /**
     * <默认构造函数>
     */
    public OperatorMng()
    {
        this.inputs = Maps.newLinkedHashMap();
        this.outputs = Maps.newLinkedHashMap();
        this.functions = Maps.newLinkedHashMap();
        this.sortedFunctions = Lists.newArrayList();
    }
    
    /**
     * 算子名称是否有效：如果不重复则有效
     * <功能详细描述>
     *
     */
    public boolean isNameValid(String opName)
    {
        LOG.debug("isNameValid enter, the opName is:{}.", opName);
        if (!inputs.containsKey(opName) && !functions.containsKey(opName))
        {
            return true;
        }
        return false;
    }
    
    /**
     * 增加源算子信息
     *
     */
    public boolean addInputStreamOperator(IRichOperator source)
    {
        if (isNameValid(source.getOperatorId()))
        {
            inputs.put(source.getOperatorId(), source);
            return true;
        }
        return false;
    }
    
    /**
     * 添加输出算子
     *
     */
    public boolean addOutputStreamOperator(IRichOperator output)
    {
        if (isNameValid(output.getOperatorId()))
        {
            outputs.put(output.getOperatorId(), output);
            return true;
        }
        return false;
    }
    
    /**
     * 添加功能算子
     *
     */
    public boolean addFunctionStreamOperator(IRichOperator operator)
    {
        if (isNameValid(operator.getOperatorId()))
        {
            functions.put(operator.getOperatorId(), operator);
            return true;
        }
        return false;
    }
    
    /**
     * 获取输出算子
     *
     */
    public List<IRichOperator> getOutputOps()
    {
        List<IRichOperator> s = new ArrayList<IRichOperator>();
        for (Entry<String, IRichOperator> m : outputs.entrySet())
        {
            s.add(m.getValue());
        }
        return s;
    }
    
    /**
     * 获得所有功能算子信息
     * <功能详细描述>
     *
     */
    public List<IRichOperator> getFunctionOps()
    {
        LOG.debug("GetFunctionOps enter.");
        List<IRichOperator> s = new ArrayList<IRichOperator>();
        for (Entry<String, IRichOperator> m : functions.entrySet())
        {
            s.add(m.getValue());
        }
        return s;
    }
    
    /**
     * 获得所有源算子信息
     *
     */
    public List<IRichOperator> getSourceOps()
    {
        List<IRichOperator> s = new ArrayList<IRichOperator>();
        for (Entry<String, IRichOperator> m : inputs.entrySet())
        {
            s.add(m.getValue());
        }
        return s;
    }
    
    /**
     * 产生有向无环图DAG
     *
     * 对有向图进行拓扑排序
     *
     * 无后继的顶点优先拓扑排序方法
     * 1、思想方法
     * 该方法的每一步均是输出当前无后继(即出度为0)的顶点。
     * 对于一个DAG，按此方法输出的序列是逆拓扑次序。
     * 因此设置一个栈(或向量)T来保存输出的顶点序列，即可得到拓扑序列。
     * 若T是栈，则每当输出顶点时，只需做入栈操作，排序完成时将栈中顶点依次出栈即可得拓扑序列。
     * 若T是向量，则将输出的顶点从T[n-1]开始依次从后往前存放，即可保证T中存储的顶点是拓扑序列。
     */
    /**
     * 生成功能算子拓扑顺序
     * <功能详细描述>
     *
     */
    public List<IRichOperator> genFunctionOpsOrder()
        throws StreamingException
    {
        LOG.info("GenFunctionOpsOrder enter.");
        validateOperators();
        
        //所有当前已删除的输出流名称
        List<String> deletedStreamName = Lists.newArrayList();
        
        //获得所有Source算子的输出流名称
        for (Entry<String, IRichOperator> et : inputs.entrySet())
        {
            deletedStreamName.add(et.getValue().getOutputStream());
        }
        
        //如果所有的Spout没有输出流，则不排序，返回NULL
        if (deletedStreamName.size() == 0)
        {
            LOG.debug("All the source operators have no output stream.");
            return null;
        }
        
        //获得当前所有未排序的功能算子名称
        Map<String, IRichOperator> functionsAndOutputs = functionAndOutputOperators();
        
        Set<String> unSortedFunctions = Sets.newHashSet();
        unSortedFunctions.addAll(functionsAndOutputs.keySet());
        
        IRichOperator fun = null;
        boolean qualified = true;
        int presortedSize = 0;
        boolean stopFlag = false;
        //在所有未排序的功能算子中遍历处理
        while (unSortedFunctions.size() > 0 && !stopFlag)
        {
            presortedSize = sortedFunctions.size();
            Iterator<String> iterator = unSortedFunctions.iterator();
            while (iterator.hasNext())
            {
                String funname = iterator.next();
                fun = functionsAndOutputs.get(funname);
                List<String> inputStreams = fun.getInputStream();
                if (inputStreams == null)
                {
                    //为空，则没有输入算子，异常，退出循环
                    break;
                }

                for (String inputname : inputStreams)
                {
                    if (!deletedStreamName.contains(inputname))
                    {
                        qualified = false;
                        break;
                    }
                }
                /*
                 * 如果该算子的所有输入流名称均出现在已删除流名称中，则
                 * 将该算子加入到排序算子列表中
                 * 并将其输出流名称加入到已删除流名称
                 * 在未排序算子名称中删除该算子名称
                 */
                if (qualified)
                {
                    sortedFunctions.add(fun);
                    if (!(fun instanceof OutputOperator))
                    {
                        /*
                             split算子有多个输出
                             这个只是暂时修改，因为目前的接口还不支持多流输出
                         */
                        //
                        if (fun instanceof SplitOp)
                        {
                            SplitOp sop = (SplitOp)fun;
                            Map<String, IEventType> outputMap = sop.getOutputSchemaMap();
                            for (String outputStreamName : outputMap.keySet())
                            {
                                deletedStreamName.add(outputStreamName);
                            }
                        }
                        else
                        {
                            deletedStreamName.add(fun.getOutputStream());
                        }
                    }
                    iterator.remove();
                }
                qualified = true;
            }
            /*
             * 如果本次遍历时没有增加任何排序的算子，即剩下的每个算子都不满足排序条件，则说明不符合DAG，跳出遍历
             */
            if (sortedFunctions.size() == presortedSize)
            {
                stopFlag = true;
            }
        }
        //不满足DAG，拓扑设置非法
        stopWhileNoAdded(stopFlag);
        stopWhileNoAllAdded(functionsAndOutputs);
        return sortedFunctions;
    }
    
    private void stopWhileNoAllAdded(Map<String, IRichOperator> functionsAndOutputs)
        throws StreamingException
    {
        if (sortedFunctions.size() != functionsAndOutputs.size())
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
            LOG.error(ErrorCode.PLATFORM_INVALID_TOPOLOGY.getFullMessage(), exception);
            throw exception;
        }
    }
    
    private void stopWhileNoAdded(boolean stopFlag)
        throws StreamingException
    {
        if (stopFlag)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
            LOG.error(ErrorCode.PLATFORM_INVALID_TOPOLOGY.getFullMessage(), exception);
            throw exception;
        }
    }
    
    private void validateOperators()
        throws StreamingException
    {
        if (null == sortedFunctions || sortedFunctions.size() > 0)
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
            LOG.error(ErrorCode.PLATFORM_INVALID_TOPOLOGY.getFullMessage(), exception);
            throw exception;
        }
    }
    
    private Map<String, IRichOperator> functionAndOutputOperators()
    {
        Map<String, IRichOperator> map = Maps.newHashMap();
        map.putAll(functions);
        map.putAll(outputs);
        return map;
    }
}
