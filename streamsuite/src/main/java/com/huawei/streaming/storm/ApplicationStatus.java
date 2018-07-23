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

package com.huawei.streaming.storm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * Application状态
 */
public enum ApplicationStatus
{
    
    /**
     * 激活状态
     */
    ACTIVE("active")
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void activeValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "active");
            LOG.error("Application " + appName + " is already active, active failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void deactiveValidate(String appName)
        {
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void rebalanceValidate(String appName)
            throws StreamingException
        {
        }
    },
    
    /**
     * 非激活状态
     */
    INACTIVE("inactive")
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void activeValidate(String appName)
        {
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void deactiveValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "deactive");
            LOG.error("Application " + appName + " is already inactive, deactive failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void rebalanceValidate(String appName)
            throws StreamingException
        {
        }
    },
    /**
     * killed状态
     */
    KILLED("killed")
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void activeValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "active");
            LOG.error("Application " + appName + " is killed, active failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void deactiveValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "deactive");
            LOG.error("Application " + appName + " is killed, deactive failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void rebalanceValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "rebalance");
            LOG.error("Application " + appName + " is killed, rebalance failed");
            throw exception;
        }
    },
    
    /**
     * rebalancing状态
     */
    REBALANCING("rebalancing")
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void activeValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "active");
            LOG.error("Application " + appName + " is rebalancing, active failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void deactiveValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "deactive");
            LOG.error("Application " + appName + " is rebalancing, deactive failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void rebalanceValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "rebalance");
            LOG.error("Application " + appName + " is rebalancing, rebalance failed");
            throw exception;
        }
    },
    
    /**
     * unknow状态
     */
    UNKNOW("unknow")
    {
        /**
         * {@inheritDoc}
         */
        @Override
        public void activeValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "active");
            LOG.error("Application " + appName + " is unknow, active failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void deactiveValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "deactive");
            LOG.error("Application " + appName + " is unknow, deactive failed");
            throw exception;
        }
        
        /**
         * {@inheritDoc}
         */
        @Override
        public void rebalanceValidate(String appName)
            throws StreamingException
        {
            StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_STATUS_ERROR, appName, this.getValue(), "rebalance");
            LOG.error("Application " + appName + " is unknow, rebalance failed");
            throw exception;
        }
    };
    
    private static final Logger LOG = LoggerFactory.getLogger(ApplicationStatus.class);
    
    private String value;
    
    private ApplicationStatus(String val)
    {
        this.value = val;
    }
    
    public String getValue()
    {
        return value;
    }
    
    /**
     * 获取app状态枚举类型
     * 根据输入字符串获取相应枚举类型
     * 
     */
    public static ApplicationStatus getType(String val)
    {
        for (ApplicationStatus s : ApplicationStatus.values())
        {
            if (s.getValue().equals(val))
            {
                return s;
            }
        }
        return UNKNOW;
    }
    
    /**
     * active操作执行检查
     */
    public abstract void activeValidate(String appName)
        throws StreamingException;
    
    /**
     * deactive操作执行检查
     */
    public abstract void deactiveValidate(String appName)
        throws StreamingException;
    
    /**
     * rebalance操作执行检查
     */
    public abstract void rebalanceValidate(String appName)
        throws StreamingException;
    
}
