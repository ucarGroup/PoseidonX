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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.FunctionInfo;
import com.huawei.streaming.cql.executor.FunctionRegistry;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.exception.ErrorCode;

/**
 * function解析内容
 * 
 */
public class FunctionContext extends BaseExpressionParseContext
{
    private static final Logger LOG = LoggerFactory.getLogger(FunctionContext.class);
    
    private String name;
    
    private boolean isDistinct = false;
    
    private List<BaseExpressionParseContext> arguments;
    
    private StreamAllColumnsContext allColumns;
    
    /**
     * <默认构造函数>
     */
    public FunctionContext()
    {
        arguments = Lists.newArrayList();
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append("(");
        sb.append(functionBodytoString());
        sb.append(")");
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpressions(walker, arguments);
    }
    
    private String functionBodytoString()
    {
        StringBuilder sb = new StringBuilder();
        
        if (isDistinct)
        {
            sb.append(" DISTINCT ");
        }
        
        if (allColumns != null)
        {
            sb.append(allColumns.toString());
        }
        
        sb.append(Joiner.on(", ").join(arguments.toArray()));
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = foundIndexsInChilds(replacer);
        replace(replacedIndex, replacer);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        FunctionInfo finfo = createFunctionInfo(name);
        FunctionExpressionDesc expression = new FunctionExpressionDesc(finfo);
        expression.setDistinct(isDistinct);
        if (allColumns != null)
        {
            expression.setSelectStar(true);
        }
        
        for (BaseExpressionParseContext arg : arguments)
        {
            expression.getArgExpressions().add(arg.createExpressionDesc(getSchemas()));
        }
        
        return expression;
    }
    
    private FunctionInfo createFunctionInfo(String functionName)
        throws SemanticAnalyzerException
    {
        FunctionRegistry funRegistry = DriverContext.getFunctions().get();
        FunctionInfo finfo = funRegistry.getFunctionInfoByFunctionName(functionName);
        if (null == finfo)
        {
            SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.FUNCTION_UNSPPORTED, functionName);
            LOG.error(ErrorCode.FUNCTION_UNSPPORTED.getFullMessage(functionName), exception);
            
            throw exception;
        }
        return finfo;
    }
    
    private List<Integer> foundIndexsInChilds(ParseContextReplacer replacer)
    {
        List<Integer> replacedIndex = Lists.newArrayList();
        for (int i = 0; i < arguments.size(); i++)
        {
            BaseExpressionParseContext child = arguments.get(i);
            if (replacer.isChildsReplaceable(child))
            {
                replacedIndex.add(i);
            }
            else
            {
                child.walkChildAndReplace(replacer);
            }
        }
        return replacedIndex;
    }
    
    private void replace(List<Integer> replacedIndex, ParseContextReplacer replacer)
    {
        BaseExpressionParseContext replacedContext = replacer.createReplaceParseContext();
        for (Integer index : replacedIndex)
        {
            arguments.set(index, replacedContext);
        }
    }
    
    public boolean isDistinct()
    {
        return isDistinct;
    }
    
    public void setDistinct(boolean isdistinct)
    {
        this.isDistinct = isdistinct;
    }
    
    public List<BaseExpressionParseContext> getArguments()
    {
        return arguments;
    }
    
    public void setArguments(List<BaseExpressionParseContext> arguments)
    {
        this.arguments = arguments;
    }
    
    public StreamAllColumnsContext getAllColumns()
    {
        return allColumns;
    }
    
    public void setAllColumns(StreamAllColumnsContext allColumns)
    {
        this.allColumns = allColumns;
    }
    
}
