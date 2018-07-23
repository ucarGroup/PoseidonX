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

package com.huawei.streaming.cql.semanticanalyzer.parser.visitor;

import java.lang.reflect.Constructor;
import java.math.BigDecimal;

import org.antlr.v4.runtime.misc.NotNull;

import com.huawei.streaming.cql.CQLConst;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.semanticanalyzer.parser.CQLParser;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ConstantContext;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * 常量遍历
 * 
 */
public class ConstantVisitor extends AbsCQLParserBaseVisitor<ConstantContext>
{
    private ConstantContext context = null;
    
    private String unaryOperator = "";
    
    /**
     * <默认构造函数>
     */
    public ConstantVisitor()
    {
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected ConstantContext defaultResult()
    {
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitUnaryOperator(@NotNull CQLParser.UnaryOperatorContext ctx)
    {
        if (ctx.getText().equals("-"))
        {
            unaryOperator = ctx.getText();
        }
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitConstIntegerValue(@NotNull CQLParser.ConstIntegerValueContext ctx)
    {
        Class< ? > type = Integer.class;
        Object value = createConstInstance(unaryOperator + ctx.getText(), type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitConstLongValue(@NotNull CQLParser.ConstLongValueContext ctx)
    {
        Class< ? > type = Long.class;
        Object value =
            createConstInstance(unaryOperator + ctx.getText().substring(0, ctx.getText().length() - 1), type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitConstFloatValue(@NotNull CQLParser.ConstFloatValueContext ctx)
    {
        Class< ? > type = Float.class;
        Object value =
            createConstInstance(unaryOperator + ctx.getText().substring(0, ctx.getText().length() - 1), type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitConstDoubleValue(@NotNull CQLParser.ConstDoubleValueContext ctx)
    {
        Class< ? > type = Double.class;
        Object value =
            createConstInstance(unaryOperator + ctx.getText().substring(0, ctx.getText().length() - 1), type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitConstBigDecimalValue(@NotNull CQLParser.ConstBigDecimalValueContext ctx)
    {
        Class< ? > type = BigDecimal.class;
        String number = ctx.getText().substring(0, ctx.getText().length() - CQLConst.I_2);
        Object value = createConstInstance(unaryOperator + number, type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitConstStingValue(@NotNull CQLParser.ConstStingValueContext ctx)
    {
        Class< ? > type = String.class;
        Object value = createConstInstance(CQLUtils.cqlStringLiteralTrim(ctx.getText()), type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public ConstantContext visitBooleanValue(@NotNull CQLParser.BooleanValueContext ctx)
    {
        Class< ? > type = Boolean.class;
        Object value = createConstInstance(ctx.getText(), type);
        context = new ConstantContext(type, value);
        return context;
    }
    
    private Object createConstInstance(String value, Class< ? > clazz)
    {
        Object resValue = null;
        try
        {
            Constructor< ? > constructor = clazz.getConstructor(String.class);
            resValue = constructor.newInstance(value);
        }
        catch (Exception e)
        {
            throw new StreamingRuntimeException(e);
        }
        return resValue;
    }
    
}
