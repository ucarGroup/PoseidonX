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

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.google.common.collect.Maps;
import com.huawei.streaming.api.AnnotationUtils;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.mapping.InputOutputOperatorMapping;
import com.huawei.streaming.cql.mapping.SimpleLexer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.CreateDataSourceAnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.*;
import com.huawei.streaming.exception.ErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * create datasource 语义分析
 *
 */
public class CreateDatasourceAnalyzer extends BaseAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(CreateDatasourceAnalyzer.class);

    private CreateDataSourceAnalyzeContext analyzeContext = null;
    
    private CreateDataSourceContext context;
    
    /**
     * <默认构造函数>
     *
     */
    public CreateDatasourceAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        super(parseContext);
        context = (CreateDataSourceContext)parseContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public AnalyzeContext analyze()
        throws SemanticAnalyzerException
    {
        analyzeContext.setDataSourceClass(getDataSourceClass());
        parseStreamProperties();
        TreeMap<String, String> conf = convertSourceSimpleConf(analyzeContext.getDataSourceConfig());
        analyzeContext.setDatasourceConfigs(conf);
        return analyzeContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected void createAnalyzeContext()
    {
        analyzeContext = new CreateDataSourceAnalyzeContext();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected AnalyzeContext getAnalyzeContext()
    {
        return analyzeContext;
    }
    
    private TreeMap<String, String> convertSourceSimpleConf(final TreeMap<String, String> sourceConf)
        throws SemanticAnalyzerException
    {
        String sourceClassName = analyzeContext.getDataSourceClass();
        return convertSimpleConf(sourceConf, sourceClassName);
    }
    
    private TreeMap<String, String> convertSimpleConf(TreeMap<String, String> serdeProperties, String deserClassName)
        throws SemanticAnalyzerException
    {
        String apiOperator = InputOutputOperatorMapping.getAPIOperatorByPlatform(deserClassName);
        if (apiOperator == null)
        {
            return serdeProperties;
        }
        
        Map<String, String> configMapping = AnnotationUtils.getConfigMapping(apiOperator);
        TreeMap<String, String> confs = Maps.newTreeMap();
        
        for (Map.Entry<String, String> et : serdeProperties.entrySet())
        {
            String fullName = et.getKey();
            String value = et.getValue();
            if (configMapping.containsKey(fullName))
            {
                fullName = configMapping.get(fullName);
            }
            confs.put(fullName, value);
        }
        
        return confs;
    }
    
    private void parseStreamProperties()
    {
        TreeMap<String, String> properties = Maps.newTreeMap();

        StreamPropertiesContext propertiesContext = context.getDataSourceProperties();
        if(propertiesContext == null)
        {
            analyzeContext.setDatasourceConfigs(properties);
            return;
        }

        List<KeyValuePropertyContext> propertyList = propertiesContext.getProperties();

        for (KeyValuePropertyContext ctx : propertyList)
        {
            String key = ctx.getKey();
            String value = ctx.getValue();
            properties.put(key, value);
        }
        
        analyzeContext.setDatasourceConfigs(properties);
    }

    private String getDataSourceClass()
        throws SemanticAnalyzerException
    {
        ClassNameContext className = context.getDataSourceClassName();
        if (className.isInnerClass())
        {
            String clazzName = className.getClassName();
            String fullName = SimpleLexer.DATASOURCE.getFullName(clazzName);
            if (fullName == null)
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_UNMATCH_OPERATOR, clazzName);
                LOG.error("The '{}' operator type does not match.", clazzName);
                throw exception;
            }
            return fullName;
        }

        return className.getClassName();
    }
}
