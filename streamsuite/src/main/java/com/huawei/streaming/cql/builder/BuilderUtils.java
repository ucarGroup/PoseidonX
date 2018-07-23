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

package com.huawei.streaming.cql.builder;

import java.text.DecimalFormat;
import java.util.List;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateDataSourceAnalyzeContext;

/**
 * 应用程序构建中用到的一些公用的方法
 * 
 */
public class BuilderUtils
{

    /**
     * CQL默认并发数
     */
    private static final int DEFAULT_PARALLEL_NUMBER = 1;
    
    private static final String BASIC_STREAM_NAME = "st_";
    
    private static final String BASIC_OPERATOR_NAME = "op";
    
    private static final String BASIC_SUBQUERY_NAME = "sub_";

    private static final String OPERATORNAME_SEPRATOR = "_";
    
    private int nowSubQueryNum = 0;
    
    private int nowStreamNum = 0;
    
    private int nowOperatorNum = 0;
    
    private StreamingConfig conf = null;
    
    private List<CreateDataSourceAnalyzeContext> datasourceDefines = Lists.newArrayList();
    
    /**
     * <默认构造函数>
     */
    public BuilderUtils()
    {
        conf = new StreamingConfig();
    }
    
    /**
     * 获取默认的并发度
     */
    public int getDefaultParallelNumber()
    {
        if (conf.containsKey(StreamingConfig.STREAMING_COMMON_PARALLEL_NUMBER))
        {
            return Integer.valueOf(conf.get(StreamingConfig.STREAMING_COMMON_PARALLEL_NUMBER).toString());
        }
        return DEFAULT_PARALLEL_NUMBER;
    }
    
    /**
     * 获取下一个子查询的名称
     */
    public String getNextSubQueryName()
    {
        nowSubQueryNum++;
        return BASIC_SUBQUERY_NAME + getNextNumber(nowSubQueryNum);
    }
    
    /**
     * 获取下一个流名称
     */
    public String getNextStreamName()
    {
        nowStreamNum++;
        return BASIC_STREAM_NAME + getNextNumber(nowStreamNum);
    }
    
    /**
     * 获取下一个算子名称
     * 
     */
    public String getNextOperatorName(String prefix)
    {
        nowOperatorNum++;
        String name = prefix == null ? BASIC_OPERATOR_NAME : prefix;
        return name + OPERATORNAME_SEPRATOR + getNextNumber(nowOperatorNum);
    }

    /**
     * 算子名称重命名
     *
     */
    public static String renameOperatorName(String oldName, String newNamePrefix)
    {
        if (Strings.isNullOrEmpty(oldName))
        {
            return oldName;
        }

        String[] operatorNames = oldName.split(OPERATORNAME_SEPRATOR);
        String order = operatorNames[operatorNames.length - 1];
        return newNamePrefix + OPERATORNAME_SEPRATOR + order;
    }

    /**
     * 添加数据源定义
     */
    public void addDataSourceDefined(CreateDataSourceAnalyzeContext context)
    {
        datasourceDefines.add(context);
    }
    
    /**
     * 根据数据源名称获取数据源定义内容
     */
    public CreateDataSourceAnalyzeContext getDataSourceDefineByName(String dataSourceName)
    {
        for (CreateDataSourceAnalyzeContext context : datasourceDefines)
        {
            if (context.getDataSourceName().equals(dataSourceName))
            {
                return context;
            }
        }
        return null;
    }
    
    private String getNextNumber(int nowNumber)
    {
        DecimalFormat formatter = new DecimalFormat("000");
        return formatter.format(nowNumber);
    }
}
