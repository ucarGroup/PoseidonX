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

package com.huawei.streaming.expression;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * <运算表达式抽象类>
 * 
 */
public abstract class OperatorBasedExpression implements IExpression
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 6369496611542821392L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(OperatorBasedExpression.class);
    
    /**
     * 运算对象
     */
    private ExpressionOperator op = null;
    
    /**
     * 左表达式
     */
    private IExpression leftExpr = null;
    
    /**
     * 右表达式
     */
    private IExpression rightExpr = null;
    
    /**
     * 表达式返回类型
     */
    private Class< ? > type = null;
    
    /**
     * <默认构造函数>
     *@param op 运算操作
     *@param leftExpr 左表达式
     *@param rightExpr 右表达式
     */
    public OperatorBasedExpression(ExpressionOperator op, IExpression leftExpr, IExpression rightExpr)
    {
        if (null == op || null == leftExpr || null == rightExpr)
        {
            String msg = "Illegal operator expression argument.";
            LOG.error(msg);
            throw new IllegalArgumentException(msg);
        }
        
        this.op = op;
        this.leftExpr = leftExpr;
        this.rightExpr = rightExpr;
    }
    
    /**
     * <返回运算操作>
     */
    public ExpressionOperator getOp()
    {
        return op;
    }
    
    /**
     * <返回左表达式>
     */
    public IExpression getLeftExpr()
    {
        return leftExpr;
    }
    
    /**
     * <返回右表达式>
     */
    public IExpression getRightExpr()
    {
        return rightExpr;
    }
    
    /**
     * 返回 type
     */
    public Class< ? > getType()
    {
        return type;
    }
    
    /**
     * 对type进行赋值
     */
    public void setType(Class< ? > type)
    {
        this.type = type;
    }
    
    public void setLeftExpr(IExpression leftExpr)
    {
        this.leftExpr = leftExpr;
    }
    
    public void setRightExpr(IExpression rightExpr)
    {
        this.rightExpr = rightExpr;
    }
}
