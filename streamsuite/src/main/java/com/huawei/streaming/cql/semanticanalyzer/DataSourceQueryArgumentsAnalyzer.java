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

package com.huawei.streaming.cql.semanticanalyzer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.DataSourceQueryArgumentsAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.DataSourceBodyContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ExpressionContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.util.GenericTokenParser;

/**
 * 解析数据源中的每一个CQLDataSource
 * <p/>
 * From子句中解析dataSource的时候，schema中的别名还未解析，
 * 所以就会数据源中的cql参数中所用到的别名就没有办法解析，
 * 所以就应该在from子句解析完毕之后，再来进行解析
 *
 */
public class DataSourceQueryArgumentsAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(DataSourceQueryArgumentsAnalyzer.class);
    
    private DataSourceQueryArgumentsAnalyzeContext analzeContext = null;
    
    private DataSourceBodyContext parseContext = null;
    
    /**
     * <默认构造函数>
     *
     */
    public DataSourceQueryArgumentsAnalyzer(ParseContext parsecontext)
        throws SemanticAnalyzerException
    {
        super(parsecontext);
        this.parseContext = (DataSourceBodyContext)parsecontext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        for (ExpressionContext arg : parseContext.getQueryarguments())
        {
            analzeContext.getQueryArguments().add(arg.toString());
        }
        return analzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        analzeContext = new DataSourceQueryArgumentsAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return analzeContext;
    }
    
}
