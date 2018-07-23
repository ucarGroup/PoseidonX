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
package com.huawei.streaming.storm.components;

import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.operator.OutputOperator;
import com.huawei.streaming.storm.StormBolt;
import com.huawei.streaming.storm.StormOutputBolt;

import backtype.storm.topology.IRichBolt;

/**
 * Streaming已经实现的算子，底层不依赖于Storm的Bolt实例构建
 * Created by h00183771 on 2015/10/26.
 */
public class CommonBoltCreator
{

    /**
     * 创建功能性算子
     */
    public static IRichBolt createFunctionBolt(IRichOperator operator){
        StormBolt bolt = new StormBolt();
        bolt.setOperator(operator);
        return bolt;
    }

    /**
     * 创建输出算子
     */
    public static IRichBolt createOutputBolt(OutputOperator operator)
    {
        StormOutputBolt outputBolt = new StormOutputBolt();
        outputBolt.setOperator(operator);
        return outputBolt;
    }
}
