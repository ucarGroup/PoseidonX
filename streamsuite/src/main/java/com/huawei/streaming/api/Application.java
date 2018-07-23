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

package com.huawei.streaming.api;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.TreeMap;

import com.huawei.streaming.api.opereators.Operator;
import com.huawei.streaming.api.opereators.OperatorTransition;
import com.huawei.streaming.api.streams.Schema;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;

/**
 * 流处理执行计划应用程序
 *
 */
public class Application
{
    
    /**
     * 应用id
     */
    @XStreamAlias("id")
    @XStreamAsAttribute
    private String applicationId = null;
    
    /**
     * 应用名称
     */
    @XStreamAlias("name")
    @XStreamAsAttribute
    private String applicationName = null;
    
    /**
     * 整个应用程序中用到的配置属性
     * 也包含用户自定义的配置属性
     * 如果属性和系统属性同名，会覆盖系统属性
     * 为了防止输出的时候配置属性遍历顺序变化导致测试结果不一致，所以改为treemap
     */
    private TreeMap<String, String> confs;
    
    /**
     * 用户自定义添加的一些文件
     */
    private String[] userFiles;
    
    /**
     * 用户自定义的函数
     * udf和udaf都在这个里面
     */
    @XStreamAlias("UDFs")
    private List<UserFunction> userFunctions;
    
    /**
     * 执行计划中的所有的schema
     */
    @XStreamAlias("Schemas")
    private List<Schema> schemas = new ArrayList<Schema>();
    
    /**
     * 执行计划中所有的操作
     * 包含输入、输出和计算操作
     */
    @XStreamAlias("Operators")
    private List<Operator> operators = null;
    
    /**
     * 整个执行计划中所有的连接线，定义了operator之间的连接关系
     */
    @XStreamAlias("Transitions")
    private List<OperatorTransition> opTransition = null;
    
    /**
     * <默认构造函数>
     *
     */
    public Application(String applicationId)
    {
        super();
        this.applicationId = applicationId;
    }
    
    public TreeMap<String, String> getConfs()
    {
        return confs;
    }
    
    public void setConfs(TreeMap<String, String> confs)
    {
        this.confs = confs;
    }
    
    public String[] getUserFiles()
    {
        return userFiles == null ? new String[] {} : (String[])userFiles.clone();
    }
    
    public void setUserFiles(String[] userFiles)
    {
        this.userFiles = Arrays.copyOf(userFiles, userFiles.length);
    }
    
    public List<UserFunction> getUserFunctions()
    {
        return userFunctions;
    }
    
    public void setUserFunctions(List<UserFunction> userFunctions)
    {
        this.userFunctions = userFunctions;
    }
    
    public List<Schema> getSchemas()
    {
        return schemas;
    }
    
    public void setSchemas(List<Schema> schemas)
    {
        this.schemas = schemas;
    }
    
    public String getApplicationId()
    {
        return applicationId;
    }
    
    public void setApplicationId(String applicationId)
    {
        this.applicationId = applicationId;
    }
    
    public String getApplicationName()
    {
        return applicationName;
    }
    
    public void setApplicationName(String applicationName)
    {
        this.applicationName = applicationName;
    }
    
    public List<Operator> getOperators()
    {
        return operators;
    }
    
    public void setOperators(List<Operator> operators)
    {
        this.operators = operators;
    }
    
    public List<OperatorTransition> getOpTransition()
    {
        return opTransition;
    }
    
    public void setOpTransition(List<OperatorTransition> opTransition)
    {
        this.opTransition = opTransition;
    }
    
}
