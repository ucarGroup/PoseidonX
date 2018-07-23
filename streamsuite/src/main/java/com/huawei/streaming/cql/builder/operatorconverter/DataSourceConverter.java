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

package com.huawei.streaming.cql.builder.operatorconverter;

import com.huawei.streaming.api.opereators.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.cql.exception.ApplicationBuildException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 数据源转换
 *
 */
public class DataSourceConverter implements OperatorConverter
{
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceConverter.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean validate(Operator op)
    {
        return op instanceof DataSourceOperator;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public BaseDataSourceOperator convert(Operator op)
        throws ApplicationBuildException
    {
        DataSourceOperator datasourceOperator = (DataSourceOperator)op;
        String rss = InputOutputOperatorMapping.getAPIOperatorByPlatform(datasourceOperator.getDataSourceClassName());
        if (null == rss)
        {
            return datasourceOperator;
        }
        
        return convertToPrivateDataSource(datasourceOperator, rss);
    }
    
    /**
     * 将通用的数据源转化为私有的数据源
     *
     */
    private BaseDataSourceOperator convertToPrivateDataSource(DataSourceOperator dataSourceOperator,
        String dataSourceClass)
        throws ApplicationBuildException
    {
        /*
         * 所有的数据源都继承自BaseDataSource，所以可以进行类型强转，数据丢失无所谓
         * 但是这里是字符串类型，没办法进行强制类型转换，只好用if else的方式进行
         */
        if (dataSourceClass.equals(RDBDataSourceOperator.class.getName()))
        {
            RDBDataSourceOperator rdbDataSourceOperator = createRDBDataSource(dataSourceOperator);
            AnnotationUtils.setConfigToObject(rdbDataSourceOperator, dataSourceOperator.getDataSourceConfig());
            dataSourceOperator.setDataSourceConfig(null);
            return rdbDataSourceOperator;
        }
        else if (dataSourceClass.equals(RedisStringDataSourceOperator.class.getName()))
        {
            RedisStringDataSourceOperator redisStringDataSourceOperator = createRedisStringDataSource(dataSourceOperator);
            AnnotationUtils.setConfigToObject(redisStringDataSourceOperator, dataSourceOperator.getDataSourceConfig());
            dataSourceOperator.setDataSourceConfig(null);
            return redisStringDataSourceOperator;
        }
        ApplicationBuildException exception =
            new ApplicationBuildException(ErrorCode.SEMANTICANALYZE_DATASOURCE_UNKNOWN, dataSourceClass);
        LOG.error("Unsupport datasource type.", exception);
        throw exception;
    }
    
    private RDBDataSourceOperator createRDBDataSource(DataSourceOperator dataSourceOperator)
    {
        RDBDataSourceOperator rdb =
            new RDBDataSourceOperator(dataSourceOperator.getId(), dataSourceOperator.getParallelNumber());
        rdb.setName(dataSourceOperator.getName());
        rdb.setArgs(dataSourceOperator.getArgs());
        rdb.setLeftStreamName(dataSourceOperator.getLeftStreamName());
        rdb.setLeftWindow(dataSourceOperator.getLeftWindow());
        rdb.setFilterAfterJoinExpression(dataSourceOperator.getFilterAfterJoinExpression());
        rdb.setQueryArguments(dataSourceOperator.getQueryArguments());
        rdb.setDataSourceSchema(dataSourceOperator.getDataSourceSchema());
        rdb.setFilterAfterAggregate(dataSourceOperator.getFilterAfterAggregate());
        rdb.setGroupbyExpression(dataSourceOperator.getGroupbyExpression());
        dataSourceOperator.setOrderBy(dataSourceOperator.getOrderBy());
        dataSourceOperator.setLimit(dataSourceOperator.getLimit());
        rdb.setOutputExpression(dataSourceOperator.getOutputExpression());
        return rdb;
    }


    private RedisStringDataSourceOperator createRedisStringDataSource(DataSourceOperator dataSourceOperator)
    {
        RedisStringDataSourceOperator rdb =
                new RedisStringDataSourceOperator(dataSourceOperator.getId(), dataSourceOperator.getParallelNumber());
        rdb.setName(dataSourceOperator.getName());
        rdb.setArgs(dataSourceOperator.getArgs());
        rdb.setLeftStreamName(dataSourceOperator.getLeftStreamName());
        rdb.setLeftWindow(dataSourceOperator.getLeftWindow());
        rdb.setFilterAfterJoinExpression(dataSourceOperator.getFilterAfterJoinExpression());
        rdb.setQueryArguments(dataSourceOperator.getQueryArguments());
        rdb.setDataSourceSchema(dataSourceOperator.getDataSourceSchema());
        rdb.setFilterAfterAggregate(dataSourceOperator.getFilterAfterAggregate());
        rdb.setGroupbyExpression(dataSourceOperator.getGroupbyExpression());
        dataSourceOperator.setOrderBy(dataSourceOperator.getOrderBy());
        dataSourceOperator.setLimit(dataSourceOperator.getLimit());
        rdb.setOutputExpression(dataSourceOperator.getOutputExpression());

        return rdb;
    }
}
