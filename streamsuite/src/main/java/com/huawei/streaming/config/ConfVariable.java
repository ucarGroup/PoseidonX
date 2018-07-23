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
package com.huawei.streaming.config;

import java.util.Locale;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * 配置属性中的变量处理
 *
 */
public class ConfVariable
{
    /**
     * 配置属性中引用变量的前缀
     */
    public static final String CONF_VARIABLE_PREFIX = "${";
    
    /**
     * 配置属性中引用变量的后缀
     */
    public static final String CONF_VARIABLE_POSTFIX = "}";
    
    /**
     * 配置属性中应用System变量的前缀
     */
    public static final String SYSTEM_PREFIX = "system:";
    
    /**
     * 配置属性中引用CQL原有配置属性的前缀
     */
    public static final String CQLCONF_PREFIX = "conf:";
    
    private static final Logger LOG = LoggerFactory.getLogger(ConfVariable.class);
    
    private ConfValueType type = ConfValueType.COMMON;
    
    private String name;
    
    /**
     * 分隔符之前的字符串
     */
    private String preStr = "";
    
    /**
     * 分隔符之后的字符串
     */
    private String postStr = "";
    
    /**
     * 构造函数
     * 获取配置属性字符串并解析类型和内部名称
     *
     */
    public ConfVariable(final String value)
    {
        name = value;
        if (Strings.isNullOrEmpty(value))
        {
            return;
        }
        
        String newValue = value.trim();
        int startIndex = newValue.indexOf(CONF_VARIABLE_PREFIX);
        int endIndex = newValue.indexOf(CONF_VARIABLE_POSTFIX);
        if (startIndex != -1 && endIndex != -1)
        {
            preStr = newValue.substring(0, startIndex);
            postStr = newValue.substring(endIndex + 1);
            
            String variableString = newValue.substring(startIndex, endIndex + 1);
            parseVariableConf(variableString);
        }
        
        //如果以前缀开头，以后缀结尾，才说明是包含变量的配置属性
        //解析过程中发生任何一场，都会还原成普通配置属性
        if (newValue.startsWith(CONF_VARIABLE_PREFIX) && newValue.endsWith(CONF_VARIABLE_POSTFIX))
        {
            parseVariableConf(newValue);
        }
    }
    
    /**
     * 设置配置属性的值
     * 主要供set方法使用
     *
     */
    public static String getValue(ConfVariable confVariable, Map<String, Object> conf, Map<String, String> userConf)
        throws StreamingException
    {
        String value = null;
        switch (confVariable.type)
        {
            case SYSTEM:
                value = System.getProperty(confVariable.getName());
                break;
            case CONF:
                value = getConfValue(confVariable.getName(), conf, userConf);
                break;
            default:
                return confVariable.getName();
        }
        return confVariable.getPreStr() + value + confVariable.getPostStr();
    }
    
    /**
     * 获取配置属性的值
     * 供Get方法使用
     * 通过get命令，就可以直接获取系统变量，环境变量
     *
     */
    public static String getKey(ConfVariable confVariable, Map<String, Object> conf, Map<String, String> userConf)
        throws StreamingException
    {
        String value = null;
        if (ConfValueType.SYSTEM == confVariable.type)
        {
            value = System.getProperty(confVariable.getName());
        }
        else
        {
            value = getConfValue(confVariable.getName(), conf, userConf);
        }
        
        return confVariable.getPreStr() + value + confVariable.getPostStr();
    }
    
    private static String getConfValue(String name, Map<String, Object> conf, Map<String, String> userConf)
        throws StreamingException
    {
        if (userConf != null && userConf.containsKey(name))
        {
            return userConf.get(name);
        }
        
        if (conf != null && conf.containsKey(name))
        {
            return conf.get(name).toString();
        }
        
        StreamingException exception = new StreamingException(ErrorCode.CONFIG_NOT_FOUND, name);
        LOG.error(ErrorCode.CONFIG_NOT_FOUND.getFullMessage(name), exception);
        throw exception;
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public ConfValueType getType()
    {
        return type;
    }
    
    public void setType(ConfValueType type)
    {
        this.type = type;
    }
    
    public String getPreStr()
    {
        return preStr;
    }
    
    public String getPostStr()
    {
        return postStr;
    }
    
    private void parseVariableConf(String value)
    {
        //直接trim，不会发生空指针，已经测试过
        String variable =
            StringUtils.removeEnd(StringUtils.removeStart(value, CONF_VARIABLE_PREFIX), CONF_VARIABLE_POSTFIX).trim();
        if (Strings.isNullOrEmpty(variable))
        {
            return;
        }

        if (variable.toLowerCase(Locale.US).startsWith(SYSTEM_PREFIX))
        {
            parseSystemVariable(variable);
            return;
        }
        
        if (variable.toLowerCase(Locale.US).startsWith(CQLCONF_PREFIX))
        {
            parseConfVariable(variable);
            return;
        }
    }
    
    private void parseConfVariable(String value)
    {
        String variable = value.substring(CQLCONF_PREFIX.length()).trim();
        if (!Strings.isNullOrEmpty(variable))
        {
            type = ConfValueType.CONF;
            name = variable;
        }
    }
    
    private void parseSystemVariable(String value)
    {
        String variable = value.substring(SYSTEM_PREFIX.length()).trim();
        if (!Strings.isNullOrEmpty(variable))
        {
            type = ConfValueType.SYSTEM;
            name = variable;
        }
    }

}
