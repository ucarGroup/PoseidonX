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

package com.huawei.streaming.cql.executor;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import com.huawei.streaming.expression.ArithmeticExpression;
import com.huawei.streaming.expression.ExpressionOperator;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.expression.LogicExpression;
import com.huawei.streaming.expression.RelationExpression;

/**
 * 二元表达式注册
 * 
 * 二元表达式就是诸如：+,-,*,/,<,>之类
 * 
 */
public class BinaryExpressionRegistry
{
    
    private static final Map<String, BinaryExpression> BINARYEXPRESSIONS =
        Collections.synchronizedMap(new LinkedHashMap<String, BinaryExpression>());
    static
    {
        //数学表达式
        registerExpression("+", ExpressionOperator.ADD, ArithmeticExpression.class);
        registerExpression("-", ExpressionOperator.SUBTRACT, ArithmeticExpression.class);
        registerExpression("*", ExpressionOperator.MULTIPLY, ArithmeticExpression.class);
        registerExpression("/", ExpressionOperator.DIVIDE, ArithmeticExpression.class);
        registerExpression("%", ExpressionOperator.MOD, ArithmeticExpression.class);
        
        //关系表达式
        registerExpression("<", ExpressionOperator.LESSTHAN, RelationExpression.class);
        registerExpression(">", ExpressionOperator.GREATERTHAN, RelationExpression.class);
        registerExpression("<=", ExpressionOperator.LESSTHAN_EQUAL, RelationExpression.class);
        registerExpression(">=", ExpressionOperator.GREATERTHAN_EQUAL, RelationExpression.class);
        registerExpression("=", ExpressionOperator.EQUAL, RelationExpression.class);
        registerExpression("==", ExpressionOperator.EQUAL, RelationExpression.class);
        registerExpression("!=", ExpressionOperator.NOT_EQUAL, RelationExpression.class);
        registerExpression("<>", ExpressionOperator.NOT_EQUAL, RelationExpression.class);
        
        //逻辑表达式
        registerExpression("and", ExpressionOperator.LOGICAND, LogicExpression.class);
        registerExpression("or", ExpressionOperator.LOGICOR, LogicExpression.class);
    }
    
    /**
     * 注册表达式
     */
    public static void registerExpression(String describe, ExpressionOperator type,
        Class< ? extends IExpression> expression)
    {
        BINARYEXPRESSIONS.put(describe, new BinaryExpression(describe, type, expression));
    }
    
    /**
     * 通过名称获取表达式
     */
    public static BinaryExpression getBinaryExpressionByName(String desc)
    {
        return BINARYEXPRESSIONS.get(desc);
    }
    
}
