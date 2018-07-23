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

import java.io.Serializable;

/**
 * 算术运算
 *
 * 
 */
public interface ICompute extends Serializable
{
    /**
     * 加法算术运算
     * 
     */
    public Number add(Number left, Number right);
    
    /**
     * 减法算术运算
     * 
     */
    public Number subtract(Number left, Number right);
    
    /**
     * 乘法算术运算
     * 
     */
    public Number multiply(Number left, Number right);
    
    /**
     * 除法算术运算
     * 
     */
    public Number divide(Number left, Number right);
    
    /**
     * 求模算术运算
     * 
     */
    public Number mod(Number left, Number right);
}
