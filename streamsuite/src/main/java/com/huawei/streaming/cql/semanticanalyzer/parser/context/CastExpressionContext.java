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

import java.util.Arrays;

import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.executor.FunctionInfo;
import com.huawei.streaming.cql.executor.FunctionRegistry;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.DataTypeExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.FunctionExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextwalker.ParseContextWalker;
import com.huawei.streaming.util.StreamingDataType;

/**
 * cast表达式解析内容
 * 
 */
public class CastExpressionContext extends BaseExpressionParseContext
{
    private BaseExpressionParseContext expression;
    
    private StreamingDataType datatype;
    
    public BaseExpressionParseContext getExpression()
    {
        return expression;
    }
    
    public void setExpression(BaseExpressionParseContext expression)
    {
        this.expression = expression;
    }
    
    public StreamingDataType getDatatype()
    {
        return datatype;
    }
    
    public void setDatatype(StreamingDataType datatype)
    {
        this.datatype = datatype;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        sb.append(" cast ");
        sb.append(" ( ");
        sb.append(expression.toString());
        sb.append(" AS ");
        sb.append(datatype.getDesc());
        sb.append(" ) ");
        return sb.toString();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void walkChild(ParseContextWalker walker)
    {
        walkExpression(walker, expression);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void walkChildAndReplace(ParseContextReplacer replacer)
    {
        if (replacer.isChildsReplaceable(expression))
        {
            expression = replacer.createReplaceParseContext();
        }
        else
        {
            expression.walkChildAndReplace(replacer);
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException
    {
        FunctionExpressionDesc funExpression = new FunctionExpressionDesc(null);
        DataTypeExpressionDesc dt = new DataTypeExpressionDesc(datatype.getWrapperClass());
        FunctionRegistry funRegistry = DriverContext.getFunctions().get();
        FunctionInfo nfinfo = funRegistry.changeCastFunctionInfo(dt.getType());
        funExpression.setFinfo(nfinfo);
        funExpression.setArgExpressions(Arrays.asList(expression.createExpressionDesc(getSchemas())));
        return funExpression;
    }
}
