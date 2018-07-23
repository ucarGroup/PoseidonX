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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import java.util.List;

import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.cql.tasks.CreateUserOperatorTask;
import com.huawei.streaming.cql.tasks.Task;

/**
 * 创建用户自定义算子 解析内容
 */
public class CreateOperatorContext extends ParseContext
{
    
    /**
     * 算子名
     */
    private String operatorName;
    
    /**
     * 用户自定义算子类
     */
    private ClassNameContext operatorClassName;
    
    /**
     * 输入schema
     */
    private InputSchemaStatementContext inputSchema;
    
    /**
     * 输出schema
     */
    private OutputSchemaStatementContext outputSchema;
    
    /**
     * 自定义算子相关属性，可选
     */
    private StreamPropertiesContext operatorProperties;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OPERATOR ").append(operatorName);
        sb.append(" AS ").append(operatorClassName);
        sb.append(inputSchema.toString());
        sb.append(outputSchema.toString());
        if (operatorProperties != null)
        {
            sb.append(operatorProperties.toString());
        }
        
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        Task task = new CreateUserOperatorTask();
        return task;
    }
    
    /**
     * 获取语义分析类
     * 该方法调用的入口发生在explaintask或者submittask中。
     * 在InsertUserOperatorTask中已经将CreateOperatorContext和InsertUserOperatorStatementContext
     * 两个类合并成了一个新类FullUserOperatorContext
     * 所以语义分析的时候不会调用CreateOperatorContext和InsertUserOperatorStatementContext的createAnalyzer接口
     * 只会调用FullUserOperatorContext中的方法
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, inputSchema);
        walkExpression(walker, outputSchema);
        walkExpression(walker, operatorProperties);
    }
    
    public String getOperatorName()
    {
        return operatorName;
    }
    
    public void setOperatorName(String operatorName)
    {
        this.operatorName = operatorName;
    }
    
    public ClassNameContext getOperatorClassName()
    {
        return operatorClassName;
    }
    
    public void setOperatorClassName(ClassNameContext operatorClassName)
    {
        this.operatorClassName = operatorClassName;
    }
    
    public InputSchemaStatementContext getInputSchema()
    {
        return inputSchema;
    }
    
    public void setInputSchema(InputSchemaStatementContext inputSchema)
    {
        this.inputSchema = inputSchema;
    }
    
    public OutputSchemaStatementContext getOutputSchema()
    {
        return outputSchema;
    }
    
    public void setOutputSchema(OutputSchemaStatementContext outputSchema)
    {
        this.outputSchema = outputSchema;
    }
    
    public StreamPropertiesContext getOperatorProperties()
    {
        return operatorProperties;
    }
    
    public void setOperatorProperties(StreamPropertiesContext operatorProperties)
    {
        this.operatorProperties = operatorProperties;
    }
    
}
