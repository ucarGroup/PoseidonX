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

package com.huawei.streaming.udfs;

import java.math.BigDecimal;
import java.util.Map;

/**
 * 求取绝对值的UDF函数
 * 绝对值原始数据什么类型，返回值就是什么数据类型
 * 
 */
@UDFAnnotation("abs")
public class Abs extends UDF
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = -2295216063348303675L;
    
    /**
     * <默认构造函数>
     *
     */
    public Abs(Map<String, String> config)
    {
        super(config);
    }
    
    /**
     * Integer 类型求取绝对值
     */
    public Integer evaluate(Integer number)
    {
        if (number == null)
        {
            return null;
        }
        
        return Math.abs(number);
    }
    
    /**
     * Long 类型求取绝对值
     */
    public Long evaluate(Long number)
    {
        if (number == null)
        {
            return null;
        }
        
        return Math.abs(number);
    }
    
    /**
     * Float 类型求取绝对值
     */
    public Float evaluate(Float number)
    {
        if (number == null)
        {
            return null;
        }
        
        return Math.abs(number);
    }
    
    /**
     * Integer 类型求取绝对值
     */
    public Double evaluate(Double number)
    {
        if (number == null)
        {
            return null;
        }
        
        return Math.abs(number);
    }
    
    /**
     * Decimal 类型求取绝对值
     */
    public BigDecimal evaluate(BigDecimal number)
    {
        if (number == null)
        {
            return null;
        }
        return number.abs();
    }
}
