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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.JoinExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.PropertyValueExpressionDesc;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.FromClauseContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;

/**
 * from子句语义分析内容
 * 
 */
public class FromClauseAnalyzeContext extends AnalyzeContext
{
    
    /**
     * 输入的schmea
     * 如果是包含了join，那么就包含多个输入
     */
    private List<Schema> inputSchemas = Lists.newArrayList();
    
    private List<String> inputStreams = Lists.newArrayList();
    
    /**
     * Join表达式
     */
    private JoinExpressionDesc joinexpression;
    
    /**
     * 每个流对应的窗口
     * key: streamname, value: window in Stream
     */
    private TreeMap<String, Window> windows = Maps.newTreeMap();
    
    /**
     * 数据进入窗口之前的过滤，暂定每个流一个过滤器。
     * 这里的键值是流的名称，如果流名称设置了别名，那么就一定会是别名
     */
    private TreeMap<String, ExpressionDescribe> filterBeForeWindow = Maps.newTreeMap();
    
    /**
     * 子查询关系映射
     * 流名称-> 子查询解析结果
     */
    private TreeMap<String, InsertAnalyzeContext> subQueryForStream = Maps.newTreeMap();
    
    private TreeMap<String, CreateStreamAnalyzeContext> subQuerySchemas = Maps.newTreeMap();
    
    private TreeMap<String, PropertyValueExpressionDesc> combineConditions = Maps.newTreeMap();
    
    private String uniDirections;
    
    private FromClauseContext fromContext = null;
    
    /**
     * 添加输入流
     */
    public void addInputStream(String streamName)
    {
        inputStreams.add(streamName);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public List<Schema> getCreatedSchemas()
    {
        List<Schema> schemas = new ArrayList<Schema>();
        for (Entry<String, InsertAnalyzeContext> et : subQueryForStream.entrySet())
        {
            String streamName = et.getKey();
            for (int i = 0; i < inputSchemas.size(); i++)
            {
                if (streamName.equals(inputSchemas.get(i).getStreamName()))
                {
                    schemas.add(inputSchemas.get(i));
                }
            }
            
            InsertAnalyzeContext sp = et.getValue();
            schemas.addAll(sp.getCreatedSchemas());
        }
        return schemas;
    }
    
    /**
     * 添加事件进入窗口之前的过滤器
     */
    public void addFilterBeforeWindow(String streamName, ExpressionDescribe filter)
    {
        filterBeForeWindow.put(streamName, filter);
    }
    
    /**
     * 添加窗口
     */
    public void addWindow(String streamName, Window win)
    {
        windows.put(streamName, win);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return fromContext.toString();
    }
    
    public List<Schema> getInputSchemas()
    {
        return inputSchemas;
    }
    
    public void setInputSchemas(List<Schema> inputSchemas)
    {
        this.inputSchemas = inputSchemas;
    }
    
    public JoinExpressionDesc getJoinexpression()
    {
        return joinexpression;
    }
    
    public void setJoinexpression(JoinExpressionDesc joinexpression)
    {
        this.joinexpression = joinexpression;
    }
    
    public List<String> getInputStreams()
    {
        return inputStreams;
    }
    
    public TreeMap<String, ExpressionDescribe> getFilterBeForeWindow()
    {
        return filterBeForeWindow;
    }
    
    public TreeMap<String, Window> getWindows()
    {
        return windows;
    }
    
    public void setWindows(TreeMap<String, Window> windows)
    {
        this.windows = windows;
    }
    
    public TreeMap<String, InsertAnalyzeContext> getSubQueryForStream()
    {
        return subQueryForStream;
    }
    
    public void setSubQueryForStream(TreeMap<String, InsertAnalyzeContext> subQueryForStream)
    {
        this.subQueryForStream = subQueryForStream;
    }
    
    public void setInputStreams(List<String> inputStreams)
    {
        this.inputStreams = inputStreams;
    }
    
    public void setFilterBeForeWindow(TreeMap<String, ExpressionDescribe> filterBeForeWindow)
    {
        this.filterBeForeWindow = filterBeForeWindow;
    }
    
    public TreeMap<String, CreateStreamAnalyzeContext> getSubQuerySchemas()
    {
        return subQuerySchemas;
    }
    
    public void setSubQuerySchemas(TreeMap<String, CreateStreamAnalyzeContext> subQuerySchemas)
    {
        this.subQuerySchemas = subQuerySchemas;
    }
    
    public TreeMap<String, PropertyValueExpressionDesc> getCombineConditions()
    {
        return combineConditions;
    }
    
    public void setCombineConditions(TreeMap<String, PropertyValueExpressionDesc> combineConditions)
    {
        this.combineConditions = combineConditions;
    }
    
    public String getUniDirections()
    {
        return uniDirections;
    }
    
    public void setUniDirections(String uniDirections)
    {
        this.uniDirections = uniDirections;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setParseContext(ParseContext parseContext)
    {
        fromContext = (FromClauseContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void validateParseContext()
        throws SemanticAnalyzerException
    {
        // TODO Auto-generated method stub
        
    }
    
}
