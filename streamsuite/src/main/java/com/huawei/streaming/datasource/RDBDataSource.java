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

package com.huawei.streaming.datasource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;

import com.huawei.streaming.encrypt.NoneEncrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.encrypt.StreamingDecrypt;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;
import com.huawei.streaming.util.StreamingDataType;
import com.huawei.streaming.util.StreamingUtils;
import com.huawei.streaming.util.datatype.DataTypeParser;

/**
 * RDB数据源
 *
 */
public abstract class RDBDataSource implements IDataSource
{
    private static final long serialVersionUID = 8056232432674642637L;
    
    private static final Logger LOG = LoggerFactory.getLogger(RDBDataSource.class);
    
    private String url;
    
    private String driver;
    
    private String username;
    
    private String password;

    private TupleEventType schema;

    private String decryptClass;

    private transient Connection connection;

    private DecryptType decryptType;

    private DataTypeParser[] parsers;

    private StreamingConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf) throws StreamingException
    {
        driver = conf.getStringValue(StreamingConfig.DATASOURCE_RDB_DRIVER);
        url = conf.getStringValue(StreamingConfig.DATASOURCE_RDB_URL);
        username = conf.getStringValue(StreamingConfig.DATASOURCE_RDB_USERNAME);
        password = conf.getStringValue(StreamingConfig.DATASOURCE_RDB_PASSWORD);
        decryptClass = conf.getStringValue(StreamingConfig.DATASOURCE_RDB_DECRYPTCLASS);
        setDecryptType(conf);
        this.config = conf;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void setSchema(TupleEventType eventType)
    {
        this.schema = eventType;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initialize() throws StreamingException
    {
        parsers = new DataTypeParser[schema.getSize()];
        Class< ? >[] attributes = schema.getAllAttributeTypes();

        for (int i = 0; i < schema.getSize(); i++)
        {
            parsers[i] = StreamingDataType.getDataTypeParser(attributes[i], config);
        }

        loadDriveClass();
        try
        {
            connection = createConnection();
        }
        catch (SQLException e)
        {
            LOG.error("Failed to create sql connection!", e);
            throw new StreamingRuntimeException("Failed to create sql connection!", e);
        }
        LOG.info("Create database connection successs.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws StreamingException
    {
        LOG.info("Start to close database connection.");
        StreamingUtils.close(connection);
        LOG.info("Close database connection success.");
    }

    private void loadDriveClass()
    {
        LOG.info("Start to initialize rdb datasource");
        try
        {
            Class.forName(driver);
        }
        catch (ClassNotFoundException e)
        {
            LOG.error("can't load JDBC class {}", driver);
            throw new StreamingRuntimeException("can't load JDBC class " + driver);
        }
        LOG.info("Finished to load driver class. and start to create connection to database.");
    }

    protected List<Object[]> parseQueryResults(ResultSet result)
        throws SQLException, StreamingException
    {
        List<Object[]> results = Lists.newArrayList();
        
        ResultSetMetaData metaData = result.getMetaData();
        if (null != metaData)
        {
            int columnCount = metaData.getColumnCount();
            while (result.next())
            {
                Object[] values = parseRowValues(result, columnCount);
                results.add(values);
            }
        }
        return results;
    }
    
    private Object[] parseRowValues(ResultSet result, int columnCount)
        throws StreamingException, SQLException
    {
        Object[] values = new Object[columnCount];
        for (int i = 0; i < columnCount; i++)
        {
            values[i] = parsers[i].createValue(result.getString(i + 1));
        }
        return values;
    }

    private Connection createConnection()
       throws SQLException, StreamingException
    {
        String newUserName = username;
        String newPassWord = password;
        StreamingDecrypt decrypt = createDecryptInstance();
        switch (decryptType)
        {
            case USER:
                newUserName = decrypt.decrypt(newUserName);
                break;
            case PASSWORD:
                newPassWord = decrypt.decrypt(newPassWord);
                break;
            case ALL:
                newUserName = decrypt.decrypt(newUserName);
                newPassWord = decrypt.decrypt(newPassWord);
                break;
            default:
                break;
        }
        return DriverManager.getConnection(url, newUserName, newPassWord);
    }

    private StreamingDecrypt createDecryptInstance() throws StreamingException
    {
        if(decryptType == DecryptType.NONE)
        {
            return new NoneEncrypt();
        }

        try
        {
            return (StreamingDecrypt)Class.forName(decryptClass).newInstance();
        }
        catch (ReflectiveOperationException e)
        {
            LOG.error("can not found decrypt class " + decryptClass, e);
            throw new StreamingException("can not found decrypt class " + decryptClass, e);
        }
    }

    /**
     * 获取数据库连接
     */
    protected Connection getConnection()
    {
        return connection;
    }

    private void setDecryptType(StreamingConfig conf) throws StreamingException
    {
        String strDecryptType = conf.getStringValue(StreamingConfig.DATASOURCE_RDB_DECRYPTTYPE);

        try
        {
            decryptType = DecryptType.valueOf(strDecryptType.toUpperCase(Locale.US));
        }
        catch (IllegalArgumentException e)
        {
            StreamingException exception= new StreamingException(ErrorCode.CONFIG_FORMAT, strDecryptType, "enum");
            LOG.error(ErrorCode.CONFIG_FORMAT.getFullMessage(strDecryptType, "enum"));
            throw exception;
        }
    }

    private enum DecryptType
    {
        /**
         * 用户名加密
         */
        USER,
        /**
         * 密码加密
         */
        PASSWORD,
        /**
         * 不加密
         */
        NONE,
        /**
         * 全部都加密
         */
        ALL;
    }
}
