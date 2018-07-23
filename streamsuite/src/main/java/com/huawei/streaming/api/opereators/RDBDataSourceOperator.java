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

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.executor.RDBSecurityConverter;
import com.huawei.streaming.cql.executor.operatorinfocreater.DataSourceInfoOperatorCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;
import com.thoughtworks.xstream.annotations.XStreamConverter;

/**
 * 关系型数据库数据源
 *
 */
@OperatorInfoCreatorAnnotation(DataSourceInfoOperatorCreator.class)
public class RDBDataSourceOperator extends BaseDataSourceOperator
{
    
    /**
     * 数据库驱动
     */
    @ConfigAnnotation(StreamingConfig.DATASOURCE_RDB_DRIVER)
    private String driver;
    
    /**
     * 数据库url
     */
    @ConfigAnnotation(StreamingConfig.DATASOURCE_RDB_URL)
    private String url;
    
    /**
     * 数据库用户名
     */
    @ConfigAnnotation(StreamingConfig.DATASOURCE_RDB_USERNAME)
    @XStreamConverter(RDBSecurityConverter.class)
    private String userName;
    
    /**
     * 数据库密码
     */
    @ConfigAnnotation(StreamingConfig.DATASOURCE_RDB_PASSWORD)
    @XStreamConverter(RDBSecurityConverter.class)
    private String password;

   /**
     * 数据库解密类
     * 目前只支持数据库密码解密
     */
    @ConfigAnnotation(StreamingConfig.DATASOURCE_RDB_DECRYPTCLASS)
    private String decryptClass;

    /**
     * 解密类型
     * 用户名、密码、全部、不解密
     * USER,PASSWORD,NONE,ALL
     */
    @ConfigAnnotation(StreamingConfig.DATASOURCE_RDB_DECRYPTTYPE)
    private String decryptType;

    /**
     * <默认构造函数>
     *
     */
    public RDBDataSourceOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public String getPassword()
    {
        return password;
    }
    
    public void setPassword(String password)
    {
        this.password = password;
    }
    
    public String getDriver()
    {
        return driver;
    }
    
    public void setDriver(String driver)
    {
        this.driver = driver;
    }
    
    public String getUrl()
    {
        return url;
    }
    
    public void setUrl(String url)
    {
        this.url = url;
    }
    
    public String getUserName()
    {
        return userName;
    }
    
    public void setUserName(String userName)
    {
        this.userName = userName;
    }

    public String getDecryptClass()
    {
        return decryptClass;
    }

    public void setDecryptClass(String decryptClass)
    {
        this.decryptClass = decryptClass;
    }

    public String getDecryptType()
    {
        return decryptType;
    }

    public void setDecryptType(String decryptType)
    {
        this.decryptType = decryptType;
    }

}
