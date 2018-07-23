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

package com.huawei.streaming.expression.arithmetic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Int类型算术运算
 * 
 * 
 */
public class ComputeInt implements ICompute
{
    
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -7191854490719798673L;
    
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(ComputeInt.class);
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Number add(Number left, Number right)
    {
        return left.intValue() + right.intValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Number subtract(Number left, Number right)
    {
        return left.intValue() - right.intValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Number multiply(Number left, Number right)
    {
        return left.intValue() * right.intValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Number divide(Number left, Number right)
    {
        if (right.intValue() == 0)
        {
            LOG.warn("The value of rightExpression is 0,the result is null.");
            return null;
        }
        
        return left.intValue() / right.intValue();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Number mod(Number left, Number right)
    {
        if (right.intValue() == 0)
        {
            LOG.warn("The value of rightExpression is 0,the result is null.");
            return null;
        }
        
        return left.intValue() % right.intValue();
    }
    
}
