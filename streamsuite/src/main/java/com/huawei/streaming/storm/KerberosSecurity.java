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

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.io.Files;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;

/**
 * Streaming访问Zookeeper安全
 *
 */
public class KerberosSecurity implements StreamingSecurity
{
    /**
     * The jass.conf for zookeeper client security login.
     */
    public static final String ZOOKEEPER_AUTH_JASSCONF = "java.security.auth.login.config";
    
    /**
     * Zookeeper quorum principal.
     */
    public static final String ZOOKEEPER_AUTH_PRINCIPAL = "zookeeper.server.principal";
    
    /**
     * java security krb5 file path
     */
    public static final String JAVA_SECURITY_KRB5_CONF = "java.security.krb5.conf";
    
    private static final Logger LOG = LoggerFactory.getLogger(KerberosSecurity.class);
    
    private static final String DEFAULT_STRING_CHARSET = "UTF-8";
    
    private static final Charset DEFAULT_CHARSET = Charset.forName(DEFAULT_STRING_CHARSET);
    
    private static final String STREAMING_JAAS_POSTFIX = ".streaming.jaas.conf";
    
    private static final String LINE_SEPARATOR = IOUtils.LINE_SEPARATOR;
    
    private boolean useKeyTab = true;
    
    private String keyTabPath;
    
    private String userPrincipal;
    
    private String zookeeperPrincipal;

    private boolean useTicketCache = false;
    
    private boolean storeKey = true;
    
    private boolean debug = false;
    
    private String jaasPath;
    
    private StreamingConfig conf;
    
    private Properties backupSecurityConf = null;
    
    private String krbFilePath = null;
    
    /**
     * 构建JaasConf文件对象
     *
     */
    public KerberosSecurity(StreamingConfig config)
        throws StreamingException
    {
        initParameters(config);
        resetSecurityType();
        
        this.conf = config;
        this.jaasPath = createJaasPath();
        this.backupSecurityConf = new Properties();
        
        backupSecurityConfs();
    }
    
    private void backupSecurityConfs()
    {
        this.backupSecurityProperties(ZOOKEEPER_AUTH_PRINCIPAL);
        this.backupSecurityProperties(ZOOKEEPER_AUTH_JASSCONF);
        this.backupSecurityProperties(JAVA_SECURITY_KRB5_CONF);
    }
    
    private void initParameters(StreamingConfig config)
        throws StreamingException
    {
        readZooKeeperPrincipal(config);
        readUserPrincipal(config);
        readKeyTabPath(config);
        readKrbConfPath(config);
    }
    
    private void resetSecurityType()
        throws StreamingException
    {
        if (userPrincipal == null && keyTabPath == null)
        {
            LOG.info("Use ticket cache security.");
            this.useKeyTab = false;
            this.useTicketCache = true;
            return;
        }
        
        LOG.info("Use keytab security.");
        if (Strings.isNullOrEmpty(userPrincipal))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.CONFIG_NOT_FOUND, StreamingConfig.STREAMING_SECURITY_USER_PRINCIPAL);
            LOG.error("User principal error.", exception);
            throw exception;
        }
        
        if (Strings.isNullOrEmpty(keyTabPath))
        {
            StreamingException exception =
                new StreamingException(ErrorCode.CONFIG_NOT_FOUND, StreamingConfig.STREAMING_SECURITY_KEYTAB_PATH);
            LOG.error("Keytab file error.", exception);
            throw exception;
        }
    }
    
    /**
     * 初始化安全配置
     *
     */
    @Override
    public void initSecurity()
        throws StreamingException
    {
        writeJaasFile();
        initAuthToSysProperty();
    }
    
    /**
     * 销毁安全配置
     *
     */
    @Override
    public void destroySecurity()
        throws StreamingException
    {
        restoreSecurityConf();
        deleteJaasFile();
    }
    
    private void restoreSecurityConf()
    {
        this.restoreSecurityProperties(ZOOKEEPER_AUTH_PRINCIPAL);
        this.restoreSecurityProperties(ZOOKEEPER_AUTH_JASSCONF);
        this.restoreSecurityProperties(JAVA_SECURITY_KRB5_CONF);
    }
    
    /**
     * 删除jaas文件
     *
     */
    private void deleteJaasFile()
        throws StreamingException
    {
        try
        {
            String tmpDir = new File(conf.getStringValue(StreamingConfig.STREAMING_TEMPLATE_DIRECTORY)).getCanonicalPath();
            File file = new File(jaasPath).getCanonicalFile();
            if (!file.getPath().startsWith(tmpDir))
            {
                LOG.error("Invalid jaas path, not in config tmp path.");
                throw new StreamingException(ErrorCode.SECURITY_INNER_ERROR);
            }
            
            if (file.isFile())
            {
                FileUtils.forceDelete(new File(jaasPath));
            }
            
        }
        catch (IOException e)
        {
            LOG.error("Failed to delete jaas file.");
            throw new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
        }
        catch (SecurityException e1)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_INNER_ERROR);
            LOG.error("Failed to get canonical pathname for cannot be accessed.", exception);
            throw exception;
        }
    }
    
    /**
     * 初始化安全相关系统变量
     *
     */
    private void initAuthToSysProperty()
        throws StreamingException
    {
        String zkJassConf = decodePath();
        System.setProperty(ZOOKEEPER_AUTH_PRINCIPAL, zookeeperPrincipal);
        System.setProperty(ZOOKEEPER_AUTH_JASSCONF, zkJassConf);
        
        if (this.krbFilePath != null)
        {
            System.setProperty(JAVA_SECURITY_KRB5_CONF, krbFilePath);
        }
    }
    
    /**
     * 文件地址重新编码，防止文件地址中有空格之类
     *
     */
    private String decodePath()
        throws StreamingException
    {
        try
        {
            return URLDecoder.decode(jaasPath, DEFAULT_STRING_CHARSET);
        }
        catch (UnsupportedEncodingException e)
        {
            LOG.error("Unsupported encode, failed to decode jaas path {}.", jaasPath);
            throw new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
        }
    }
    
    /**
     * 写入jaas文件
     *
     */
    private void writeJaasFile()
        throws StreamingException
    {
        try
        {
            String jaasContext = createJaasContext();
            Files.write(jaasContext, new File(jaasPath), DEFAULT_CHARSET);
        }
        catch (IOException e)
        {
            LOG.error("Failed to create jaas file.");
            throw new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
        }
    }
    
    /**
     * 创建jaas文件内容
     *
     */
    private String createJaasContext()
    {
        if (useKeyTab)
        {
            return createKeyTabContext();
        }
        return createCacheContext();
    }
    
    /*
     * 创建人机账户登陆的jaas文件内容
     */
    private String createCacheContext()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Client {").append(LINE_SEPARATOR);
        sb.append("com.sun.security.auth.module.Krb5LoginModule required").append(LINE_SEPARATOR);
        sb.append("useKeyTab=" + useKeyTab).append(LINE_SEPARATOR);
        sb.append("useTicketCache=" + useTicketCache + ";").append(LINE_SEPARATOR);
        sb.append("};").append(LINE_SEPARATOR);
        sb.append("StormClient {").append(LINE_SEPARATOR);
        sb.append("com.sun.security.auth.module.Krb5LoginModule required").append(LINE_SEPARATOR);
        sb.append("useKeyTab=" + useKeyTab).append(LINE_SEPARATOR);
        sb.append("useTicketCache=" + useTicketCache + ";").append(LINE_SEPARATOR);
        sb.append("};");
        return sb.toString();
    }
    
    /*
     * 创建机机账户登陆的jaas文件内容
     */
    private String createKeyTabContext()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("Client {").append(LINE_SEPARATOR);
        sb.append("com.sun.security.auth.module.Krb5LoginModule required").append(LINE_SEPARATOR);
        sb.append("useKeyTab=" + useKeyTab).append(LINE_SEPARATOR);
        sb.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
        sb.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
        sb.append("useTicketCache=" + useTicketCache).append(LINE_SEPARATOR);
        sb.append("storeKey=" + storeKey).append(LINE_SEPARATOR);
        sb.append("debug=" + debug + ";").append(LINE_SEPARATOR);
        sb.append("};").append(LINE_SEPARATOR);
        sb.append("StormClient {").append(LINE_SEPARATOR);
        sb.append("com.sun.security.auth.module.Krb5LoginModule required").append(LINE_SEPARATOR);
        sb.append("useKeyTab=" + useKeyTab).append(LINE_SEPARATOR);
        sb.append("keyTab=\"" + keyTabPath + "\"").append(LINE_SEPARATOR);
        sb.append("principal=\"" + userPrincipal + "\"").append(LINE_SEPARATOR);
        sb.append("useTicketCache=" + useTicketCache).append(LINE_SEPARATOR);
        sb.append("storeKey=" + storeKey).append(LINE_SEPARATOR);
        sb.append("debug=" + debug + ";").append(LINE_SEPARATOR);
        sb.append("};");
        return sb.toString();
    }
    
    /**
     * 读取zookeeper的principal
     *
     */
    private void readZooKeeperPrincipal(StreamingConfig config)
        throws StreamingException
    {
        Object zkPrincipal = config.get(StreamingConfig.STREAMING_SECURITY_ZOOKEEPER_PRINCIPAL);
        
        if (zkPrincipal == null)
        {
            StreamingException exception =
                new StreamingException(ErrorCode.CONFIG_NOT_FOUND,
                    StreamingConfig.STREAMING_SECURITY_ZOOKEEPER_PRINCIPAL);
            LOG.error("Can't find zk principal in config.", exception);
            throw exception;
        }
        
        this.zookeeperPrincipal = zkPrincipal.toString();
    }
    
    /**
     * 读取用户principal，以此作为是否启用安全的标志
     *
     */
    private void readUserPrincipal(StreamingConfig config)
        throws StreamingException
    {
        Object principal = config.get(StreamingConfig.STREAMING_SECURITY_USER_PRINCIPAL);
        this.userPrincipal = principal == null ? null : principal.toString();
    }
    
    /**
     * 从配置属性中读取keytab文件的地址
     * 如果keytab文件不存在，就说明使用人机账户登陆
     *
     */
    private void readKeyTabPath(StreamingConfig config)
        throws StreamingException
    {
        Object keyTablePath = config.get(StreamingConfig.STREAMING_SECURITY_KEYTAB_PATH);
        this.keyTabPath = keyTablePath == null ? null : formatPath(getKeyTablePath(keyTablePath.toString()));
    }
    
    /**
     * 从配置属性中读取krb.conf文件的地址
     *
     */
    private void readKrbConfPath(StreamingConfig config)
        throws StreamingException
    {
        Object krbPath = config.get(StreamingConfig.STREAMING_SECURITY_KRBCONF_PATH);
        this.krbFilePath = krbPath == null ? null : formatPath(getKeyTablePath(krbPath.toString()));
    }
    
    private String createJaasPath() throws StreamingException
    {
        UUID uuid = UUID.randomUUID();
        String randomName = uuid.toString().replace("-", "");
        String tmpDir = conf.getStringValue(StreamingConfig.STREAMING_TEMPLATE_DIRECTORY);
        
        try
        {
            tmpDir = new File(tmpDir).getCanonicalPath();
        }
        catch (IOException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_INNER_ERROR);
            LOG.error("Failed to get canonical pathname for io error.", exception);
            throw exception;
        }
        catch (SecurityException e1)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_INNER_ERROR);
            LOG.error("Failed to get canonical pathname for cannot be accessed.", exception);
            throw exception;
        }
        
        String jaasPath =  tmpDir + File.separator + randomName + STREAMING_JAAS_POSTFIX;
        return jaasPath;
    }
    
    private String getKeyTablePath(String keyTabPath)
        throws StreamingException
    {
        try
        {
            return new File(keyTabPath).getCanonicalPath();
        }
        catch (IOException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_KEYTAB_PATH_ERROR, keyTabPath);
            LOG.error("Failed to get canonical pathname for io error.", exception);
            throw exception;
        }
        catch (SecurityException e1)
        {
            StreamingException exception = new StreamingException(ErrorCode.SECURITY_KEYTAB_PATH_ERROR, keyTabPath);
            LOG.error("Failed to get canonical pathname for cannot be accessed.", exception);
            throw exception;
        }
    }
    
    private String formatPath(String path)
    {
        return path.replace("\\", "\\\\");
    }

    private void backupSecurityProperties(String propertyKey)
    {
        if (System.getProperty(propertyKey) == null)
        {
            backupSecurityConf.put(propertyKey, "");
        }
        else
        {
            backupSecurityConf.put(propertyKey, System.getProperty(propertyKey));
        }
    }
    
    private void restoreSecurityProperties(String propertyKey)
    {
        if (Strings.isNullOrEmpty(backupSecurityConf.getProperty(propertyKey)))
        {
            System.clearProperty(propertyKey);
        }
        else
        {
            System.setProperty(propertyKey, backupSecurityConf.getProperty(propertyKey));
        }
    }
}
