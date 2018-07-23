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

import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.operator.OutputOperator;
import com.huawei.streaming.storm.StormConf;

import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.IRichSpout;

/**
 * 根据算子的不同，创建Storm不同的spout和bolt
 * Created by h00183771 on 2015/10/26.
 */
public class ComponentCreator
{
    /**
     * 根据Streaming的算子创建Storm Spout实例
     *
     */
    public static IRichSpout createSpout(IRichOperator operator) throws StreamingException
    {
        return CommonSpoutCreator.create(operator);
    }


    /**
     * 根据Streaming算子创建Storm Bolt实例
     *
     */
    public static IRichBolt createBolt(IRichOperator operator, StormConf stormConf) throws StreamingException
    {
        if (operator instanceof OutputOperator)
        {
            return createOutputBolt((OutputOperator)operator, stormConf);
        }

        return CommonBoltCreator.createFunctionBolt(operator);
    }

    private static IRichBolt createOutputBolt(OutputOperator operator, StormConf stormConf) throws StreamingException
    {
        return CommonBoltCreator.createOutputBolt(operator);
    }


}
