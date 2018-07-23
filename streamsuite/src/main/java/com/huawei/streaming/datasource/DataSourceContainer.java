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


import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * CQL数据源接口
 * 
 */
public class DataSourceContainer implements Serializable
{
    private static final long serialVersionUID = 3954936459987221892L;
    
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceContainer.class);
    
    /**
     * schema一旦设定好，就再也不会发生任何改变
     */
    private TupleEventType schema;

    /**
     * 数据源的查询参数列表
     */
    private String[] queryArgs;
    
    /**
     * CQL表达式参数列表
     */
    private Set<String> cqlQueryExpressions;

    private IDataSource dataSource;

    private List<Object[]> emptyResult = new ArrayList<Object[]>();
    
    /**
     * 设置查询参数
     */
    public void setQueryArguments(String[] queryArguments) throws StreamingException
    {
        cqlQueryExpressions = Sets.newHashSet();
        queryArgs = queryArguments;
        parseQueryArguments(queryArgs);
    }

    /**
     * 设置数据源实现
     */
    public void setDataSource(IDataSource iDataSource)
    {
        this.dataSource = iDataSource;
    }

    /**
     * 设置schema
     */
    public void setSchema(TupleEventType eventType)
    {
        this.schema = eventType;
    }

    /**
     * 运行时的初始化接口
     *
     */
    public void initialize() throws StreamingException
    {
        dataSource.initialize();
    }

    /**
     * 数据源查询接口
     * 
     * 已经经过了入参和返回值的检查，确保了输入参数和返回值不可能为Null
     * 
     */
    public List< Object[] > evaluate(Map<String, Object> cqlExpressionValues)
        throws StreamingException
    {
        validateEvaluateArguments(cqlExpressionValues);
        List< Object > replacedQueryArguments = createReplacedQueryArguments(cqlExpressionValues);
        List< Object[] > evaluateResults = dataSource.execute(replacedQueryArguments);
        return evaluateResults == null ? emptyResult : evaluateResults;
    }

    /**
     * 运行时的销毁接口
     *
     */
    public void destroy() throws StreamingException
    {
        dataSource.destroy();
    }

    /**
     * 获取查询参数中所有的CQL参数列表
     * 
     * 该方法是提供给上层计算算子使用的，
     * 计算通过该方法获取每个需要计算的表达式的值，
     * 然后将计算结果组织成Map<String,Object>的形式，用来调用evaluate方法。
     * 
     * 每个"${}"中的内容为一项参数
     */
    public String[] getCQLQueryArguments()
    {
        return cqlQueryExpressions.toArray(new String[cqlQueryExpressions.size()]);
    }

    /**
     * 获取数据源的schema
     */
    public TupleEventType getEventType()
    {
        return schema;
    }

    /**
     * 在查询之前，替换CQL查询结果
     *          必须是新new出来的，绝对不要影响到原有参数
     */
    private List< Object > createReplacedQueryArguments(Map<String, Object> cqlExpressionValues)
        throws StreamingException
    {
        List< Object > replacedResults = Lists.newArrayList();
        for (String arg : queryArgs)
        {
            replacedResults.add(cqlExpressionValues.get(arg));
        }
        return replacedResults;
    }
    
    /**
     * 校验查询参数长度是否和定义一致
     */
    private void validateEvaluateArguments(Map<String, Object> cqlExpressionValues)
    {
        if (cqlExpressionValues.size() != getEvaluateArgumentsLength())
        {
            LOG.error("evaluate arguments size doesn't same with dataSource. dataSource : {}, evaluate size : {}",
                getEvaluateArgumentsLength(),
                cqlExpressionValues.size());
            throw new StreamingRuntimeException("evaluate arguments size doesn't same with dataSource. dataSource : "
                + getEvaluateArgumentsLength() + ", evaluate size : " + cqlExpressionValues.size());
        }
    }
    
    /**
     * 获取数据源执行的参数长度
     * 数据源执行参数长度从数据源参数列表中获取，
     * 参数长度取决于数据源参数列表中的"${}"数量
     */
    private int getEvaluateArgumentsLength()
    {
        datasourceArgumentsValidate();
        //handler不需要判断是否为空，arguments已经判断，handler就不可能为空
        return getCQLQueryArguments().length;
    }
    
    /**
     * 数据源参数校验
     */
    private void datasourceArgumentsValidate()
    {
        if (queryArgs == null)
        {
            LOG.error("dataSource arguments must be can't be null!");
            throw new StreamingRuntimeException("dataSource arguments must be can't be null!");
        }
    }

    /**
     * 设置数据源参数 
     * 数据源参数就是QUERY中用括号括起来的参数
     * 
     * 根据这些参数，数据源就可以知道那个参数是关键的查询语句，那些属于过滤条件。
     * 执行参数放在关键查询语句中，用大括号"${}"括起来，
     * 比如：
     * QUERY("select rid as id,rname,rtype from rdbtable where id = '${s.id}'")
     * QUERY("f1:*,*,f1:c1","${concat('A#',s.id)}","${concat('A#',s.id)}","filter")
     * 
     * 大括号"${}"内部所有内容，都属于CQL的计算范围，包括表达式和函数计算
     * 比如：
     * QUERY("select rid as id,rname,rtype from rdbtable where id = '${s.id}'")
     * s.id就是CQL的查询参数，这条语句中只有一个CQL表达式参数，
     * 所以evaluate接口的数组长度也必须是1
     * 
     */
    private void parseQueryArguments(String[] args)
        throws StreamingException
    {
        for (String arg : queryArgs)
        {
            cqlQueryExpressions.add(arg);
        }
    }
}
