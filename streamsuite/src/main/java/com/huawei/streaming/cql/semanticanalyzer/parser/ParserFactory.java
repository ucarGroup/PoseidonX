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

package com.huawei.streaming.cql.semanticanalyzer.parser;

/**
 * 语法解析生成器工厂
 * 由于执行计划文件的存在，所以在执行器中
 * 存在对于 select子句、where条件或者join条件之类的分步骤解析
 * 所以就要针对不同的需求，实例化不同的解析器。
 * 
 */
public class ParserFactory
{
    /**
     * 创建完整的CQL解析器
     * 不论是select还是create语句，只要是完整的一句话就可以
     * 但是不包含select的子句或者where的子句之类
     */
    public static IParser createApplicationParser()
    {
        return new ApplicationParser();
    }
    
    /**
     * 创建select子句的解析器，select子句不带select关键字
     */
    public static IParser createSelectClauseParser()
    {
        return new SelectClauseParser();
    }
    
    /**
     * 创建groupby子句的解析器，不带groupby关键字
     */
    public static IParser createGroupbyClauseParser()
    {
        return new GroupbyClauseParser();
    }
    
    /**
     * 创建orderby子句解析器，不带orderby关键字
     */
    public static IParser createOrderbyClauseParser()
    {
        return new OrderbyClauseParser();
    }
    
    /**
     * 数据源参数解析器
     */
    public static IParser createDataSourceArgumentsParser()
    {
        return new DataSourceArgumentsParser();
    }
}
