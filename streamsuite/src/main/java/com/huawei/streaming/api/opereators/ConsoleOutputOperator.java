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

package com.huawei.streaming.api.opereators;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.config.StreamingConfig;

/**
 * 将计算结果打印在控制台的输出入口，一般用在本地测试
 * 
 */
public class ConsoleOutputOperator extends InnerOutputSourceOperator
{

    /**
     * 打印间隔
     * 每隔多少个数字打印一次。
     */
    @ConfigAnnotation(StreamingConfig.OPERATOR_CONSOLEPRINT_FREQUENCE)
    private Integer printFrequence;
    
    /**
     * <默认构造函数>
     */
    public ConsoleOutputOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }

    public Integer getPrintFrequence()
    {
        return printFrequence;
    }
    
    public void setPrintFrequence(Integer printFrequence)
    {
        this.printFrequence = printFrequence;
    }
}
