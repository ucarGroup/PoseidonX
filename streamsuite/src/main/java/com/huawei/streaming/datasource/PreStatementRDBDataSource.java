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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.StreamingUtils;

/**
 * 使用JDBC PreparedStatement的数据库数据源算子
 * 因为原有的Statement方法可能存在SQL注入风险
 * 但是有的数据库没有实现PreparedStatement方法，
 * 所以还是保留原来Statement的实现
 *
 */
public class PreStatementRDBDataSource extends RDBDataSource
{
    private static final Logger LOG = LoggerFactory.getLogger(PreStatementRDBDataSource.class);

    private static final long serialVersionUID = 1049038073912137943L;

    private transient PreparedStatement preStatement;

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object[]> execute(List<Object> replacedQueryArguments)
        throws StreamingException
    {
        validateQueryArguments(replacedQueryArguments);
        ResultSet result = null;
        try
        {
            createStatementIfNotExists(replacedQueryArguments);
            setStatementParameters(replacedQueryArguments);
            result = preStatement.executeQuery();
            return parseQueryResults(result);
        }
        catch (SQLException e)
        {
            preStatement = null;
            LOG.error("failed to execute sql query.", e);
            throw new StreamingException("failed to execute sql query.", e);
        }
        finally
        {
            StreamingUtils.close(result);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() throws StreamingException
    {
        LOG.info("Start to close prepare statement.");
        StreamingUtils.close(preStatement);
        LOG.info("Success to close prepare statement.");
        super.destroy();
    }

    private void setStatementParameters(List< Object > replacedQueryArguments) throws SQLException, StreamingException
    {
        preStatement.clearParameters();
        LOG.debug("Start to set prepare statement parameters.");
        if (replacedQueryArguments.size() <= 1)
        {
            LOG.debug("Can not found replaced query arguments, does not need to set prepare statement parameters.");
            return;
        }

        for (int i = 1; i < replacedQueryArguments.size(); i++)
        {
            validateNullQueryParameters(replacedQueryArguments.get(i));
            preStatement.setObject(i, replacedQueryArguments.get(i));
        }
        LOG.debug("Success to set prepare statement parameters.");
    }

    private void validateNullQueryParameters(Object replacedQueryArgument) throws StreamingException
    {
        if (replacedQueryArgument == null)
        {
            LOG.error("Null argument is not allowed in RDBDataSource.");
            throw new StreamingException("Null argument is not allowed in RDBDataSource.");
        }
    }

    private void createStatementIfNotExists(List< Object > replacedQueryArguments) throws SQLException
    {
        if (preStatement != null)
        {
            return;
        }
        
        LOG.info("Start to create prepare statement.");
        preStatement = getConnection().prepareStatement(replacedQueryArguments.get(0).toString());
        LOG.info("Success to create prepare statement.");
    }

    private void validateQueryArguments(List< Object > replacedQueryArguments) throws StreamingException
    {
        if (replacedQueryArguments == null || replacedQueryArguments.size() < 0)
        {
            LOG.error("Query arguments in RDBDataSource can not be null.");
            throw new StreamingException("Query arguments in RDBDataSource can not be null.");
        }

        if (replacedQueryArguments.get(0) == null)
        {
            LOG.error("Query sql in RDBDataSource can not be null.");
            throw new StreamingException("Query sql in RDBDataSource can not be null.");
        }
    }

}
