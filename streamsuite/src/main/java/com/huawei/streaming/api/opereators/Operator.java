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
package com.huawei.streaming.api.opereators;

import java.util.TreeMap;

import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * 每个计算单元成为一个Operator
 * 定义了各类操作
 * 分为两种操作：
 * SourceOperator
 * InnerFunctionOperator
 * 
 */
public class Operator
{
    /**
     * 算子id
     * 算子 ID是各个算子之间的标志，要求名称必须不一样。
     */
    @XStreamAsAttribute
    private String id;
    
    /**
     * 算子名称
     * 可选字段
     */
    @XStreamAsAttribute
    @XStreamOmitField
    private String name;
    
    /**
     * 并行度
     * 设置该属性为xml的属性
     */
    @XStreamAsAttribute
    @XStreamAlias("parallel")
    private int parallelNumber;
    
    /**
     * 每个operator的参数都在这里放着
     * 因为执行器不知道每个operator中到底需要哪些参数，所以只能都放在map中
     * 由上层客户端进行填充，并在底层运行时检测
     * 为了防止输出的时候配置属性遍历顺序变化导致测试结果不一致，所以改为treemap
     */
    @XStreamAlias("properties")
    private TreeMap<String, String> args;
    
    /**
     * <默认构造函数>
     */
    public Operator(String id, int parallelNumber)
    {
        super();
        this.id = id;
        this.name = id;
        this.parallelNumber = parallelNumber;
    }
    
    public String getId()
    {
        return id;
    }
    
    public void setId(String id)
    {
        this.id = id;
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public TreeMap<String, String> getArgs()
    {
        return args;
    }
    
    public void setArgs(TreeMap<String, String> args)
    {
        this.args = args;
    }
    
    public int getParallelNumber()
    {
        return parallelNumber;
    }
    
    public void setParallelNumber(int parallelNumber)
    {
        this.parallelNumber = parallelNumber;
    }
    
}
