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

package com.huawei.streaming.common;

import java.io.Serializable;
import java.security.SecureRandom;

/**
 * 根据数据类型随机生成要求数据
 * <功能详细描述>
 * 
 */
public class RandomValueGen implements Serializable
{
    
    /**
     * 注释内容
     */
    private static final long serialVersionUID = 6215371572252592802L;
    
    private static final int INTEGER_RANGE = 10000;
    
    private int incremental = 0;
    
    private SecureRandom ran = new SecureRandom();
    
    /**
     * 随机生成整数
     * <功能详细描述>
     */
    public int getInteger(int range, boolean negative)
    {
        if (range > 0)
        {
            if (!negative)
            {
                return ran.nextInt(range);
            }
            return ran.nextInt(range) * (ran.nextBoolean() == true ? 1 : -1);
        }
        if (!negative)
        {
            return ran.nextInt(INTEGER_RANGE);
        }
        return ran.nextInt(INTEGER_RANGE) * (ran.nextBoolean() == true ? 1 : -1);
    }
    
    /**
     * 随机生成双精度数
     * <功能详细描述>
     */
    public double getDouble(int range, boolean negative)
    {
        if (range > 0)
        {
            if (!negative)
            {
                return range * ran.nextDouble();
            }
            return range * ran.nextDouble() * (ran.nextBoolean() == true ? 1 : -1);
        }
        if (!negative)
        {
            return INTEGER_RANGE * ran.nextDouble();
        }
        return INTEGER_RANGE * ran.nextDouble() * (ran.nextBoolean() == true ? 1 : -1);
    }
    
    /**
     * 随机生成浮点数
     * <功能详细描述>
     */
    public double getFloat(int range, boolean negative)
    {
        if (range > 0)
        {
            if (!negative)
            {
                return range * ran.nextFloat();
            }
            return range * ran.nextFloat() * (ran.nextBoolean() == true ? 1 : -1);
        }
        if (!negative)
        {
            return INTEGER_RANGE * ran.nextFloat();
        }
        return INTEGER_RANGE * ran.nextFloat() * (ran.nextBoolean() == true ? 1 : -1);
    }
    
    /**
     * 随机生成BOOL值
     * <功能详细描述>
     */
    public boolean getBoolean()
    {
        return ran.nextBoolean();
    }
    
    /**
     * 随机生成long数
     * <功能详细描述>
     */
    public long getLong()
    {
        return ran.nextLong();
    }
    
    /**
     * 生成字串
     * <功能详细描述>
     */
    public String getString(String base, boolean prefix)
    {
        if (prefix)
        {
            return base + ran.nextInt();
        }
        return ran.nextInt() + base;
    }
    
    /**
     * 生成递增整数
     * <功能详细描述>
     */
    public int getIncremantal()
    {
        if (incremental == Integer.MAX_VALUE)
        {
            incremental = 0;
        }
        return incremental++;
    }
}
