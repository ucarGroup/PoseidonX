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
package com.huawei.streaming.cql;

import java.util.ArrayList;
import java.util.List;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.hooks.*;
import com.huawei.streaming.cql.semanticanalyzer.parser.IParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.ParserFactory;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.cql.tasks.Task;
import com.huawei.streaming.cql.tasks.TaskFactory;
import com.huawei.streaming.exception.ErrorCode;

/**
 * CQL解析处理的总类
 * <p/>
 * 命令提交的时候，各类命令都可以无序提交
 * 可以通过explain的方式查看当前提交的命令构建的应用程序结果
 * 并且将应用程序导出为执行计划并对执行计划进行修改
 * <p/>
 * 可以通过load命令导入执行计划
 * <p/>
 * 可以通过submit命令提交当前的应用程序。
 * <p/>
 * load命令和 submit命令都会导致driver对象中cql做出的临时修改清空，但是explain不会。
 * <p/>
 * explain和submit命令都会导致对当前已经提交的cql语句进行编译生成执行计划。
 * <p/>
 * 提交load命令之前和submit命令之后，都会清空所有的临时变量
 * 允许直接submit而不经过explain，这样，系统就会全部使用默认参数
 * <p/>
 * 通过load提交的应用程序，没办法修改，只能直接submit
 * 但是在命令行中提交的cql语句，只要没有submit，就都可以进行修改。
 *
 */
public class Driver implements DriverRunHook
{
    private List<DriverRunHook> driverRunHooks = new ArrayList<DriverRunHook>();
    
    private List<SemanticAnalyzeHook> analyzeHooks = new ArrayList<SemanticAnalyzeHook>();
    
    private DriverContext context;
    
    private IParser parser;
    
    private StreamingConfig config = null;
    
    /**
     * <默认构造函数>
     */
    public Driver(CQLClient cqlClient)
    {
        config = new StreamingConfig();
        context = new DriverContext(cqlClient);
        parser = ParserFactory.createApplicationParser();
        
        driverRunHooks.add(new DriverCleanerHook());
        
        analyzeHooks.add(new CreateStreamAnalyzehook());
        analyzeHooks.add(new CreateDatasourceAnalyzehook());
        analyzeHooks.add(new CommondAnalyzehook());
        analyzeHooks.add(new SelectsAnalyzeHook());
        analyzeHooks.add(new InsertAnalyzeHook());
        analyzeHooks.add(new PreviousValidateHook());
        analyzeHooks.add(new InsertUserOperatorAnalyzeHook());
        analyzeHooks.add(new MultiInsertAnalyzeHook());
    }
    
    /**
     * Returns a string representation of the object. In general, the
     * {@code toString} method returns a string that
     * "textually represents" this object. The result should
     * be a concise but informative representation that is easy for a
     * person to read.
     * It is recommended that all subclasses override this method.
     * <p/>
     * The {@code toString} method for class {@code Object}
     * returns a string consisting of the name of the class of which the
     * object is an instance, the at-sign character `{@code @}', and
     * the unsigned hexadecimal representation of the hash code of the
     * object. In other words, this method returns a string equal to the
     * value of:
     * <blockquote>
     * <pre>
     * getClass().getName() + '@' + Integer.toHexString(hashCode())
     * </pre></blockquote>
     *
     */
    @Override
    public String toString()
    {
        return super.toString();
    }
    
    /**
     * CQL运行起点
     * <p/>
     * 1、编译
     * 2、语义分析
     * 4、命令执行
     * 5、返回结果
     * <p/>
     * 除了一些查询性的命令之外，其他是不会有结果的。
     * <p/>
     * 提交load命令之前和submit命令之后，都会清空所有的临时变量
     * <p/>
     * 每次提交新的CQL语句，都会导致已经完成解析的应用程序信息被清空。
     * 因为新提交的逻辑，都会导致应用程序发生变化。
     *
     */
    public void run(String cql)
        throws CQLException
    {
        ParseContext parseContext = parser.parse(cql);
        if (parseContext == null)
        {
            CQLException exception = new CQLException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        saveAllChangeableCQLs(cql, parseContext);
        preDriverRun(context, parseContext);
        try
        {
            executeTask(parseContext);
        }
        finally
        {
            postDriverRun(context, parseContext);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void preDriverRun(DriverContext cxt, ParseContext parseContext)
    {
        for (DriverRunHook hook : driverRunHooks)
        {
            hook.preDriverRun(cxt, parseContext);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void postDriverRun(DriverContext cxt, ParseContext parseContext)
    {
        for (DriverRunHook hook : driverRunHooks)
        {
            hook.postDriverRun(cxt, parseContext);
        }
    }
    
    /**
     * 获取查询结果
     * 这个语句只适用于每次cql下发之后的结果查看
     * 如果cql语句是一般的流定义或者select语句，那么查询结果为空
     * 如果是get、或者show、describe之类的命令，那么就可以查询到结果
     *
     */
    public CQLResult getResult()
    {
        return context.getQueryResult();
    }
    
    /**
     * 清空driver中的内容
     *
     */
    public void clean()
    {
        context.clean();
        this.config = new StreamingConfig();
    }
    
    /**
     * 获取Driver的实时解析内容
     *
     */
    public DriverContext getContext()
    {
        return context;
    }
    
    private void executeTask(ParseContext parseContext)
        throws CQLException
    {
        mergeConfs();
        Task task = TaskFactory.createTask(context, parseContext, config, analyzeHooks);
        task.execute(parseContext);
        context.setQueryResult(task.getResult());
    }


    private void mergeConfs()
    {
        if(context.getUserConfs() != null)
        {
            config.putAll(context.getUserConfs());
        }
    }

    /**
     * 保存所有对查询结果会造成影响的sql语句
     *
     */
    private void saveAllChangeableCQLs(String cql, ParseContext parseContext)
    {
        if (CQLUtils.isChangeableCommond(parseContext))
        {
            context.addCQLs(cql);
        }
    }
}
