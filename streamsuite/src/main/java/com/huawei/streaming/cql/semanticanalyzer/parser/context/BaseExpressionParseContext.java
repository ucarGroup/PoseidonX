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

package com.huawei.streaming.cql.semanticanalyzer.parser.context;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.BaseAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.SemanticAnalyzer;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc.ExpressionDescribe;
import com.huawei.streaming.cql.semanticanalyzer.parsecontextreplacer.ParseContextReplacer;
import com.huawei.streaming.cql.tasks.Task;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 基础表达式解析内容
 * 
 */
public abstract class BaseExpressionParseContext extends ParseContext
{
    private static final Logger LOG = LoggerFactory.getLogger(BaseExpressionParseContext.class);
    
    private BaseExpressionParseContext leftExpression;
    
    private List<Schema> schemas;
    
    /**
     * 遍历子表达式，如果匹配就替换
     */
    public abstract void walkChildAndReplace(ParseContextReplacer replacer);
    
    /**
     * 创建表达式描述信息
     */
    protected abstract ExpressionDescribe createExpressionDesc()
        throws SemanticAnalyzerException;
    
    /**
     * 创建表达式描述
     */
    public ExpressionDescribe createExpressionDesc(List<Schema> allSchemas)
        throws SemanticAnalyzerException
    {
        schemas = allSchemas;
        return createExpressionDesc();
    }
    
    protected void setLeftExpression(BaseExpressionParseContext left)
    {
        leftExpression = left;
    }
    
    protected BaseExpressionParseContext getLeftExpression()
    {
        return leftExpression;
    }
    
    /**
     * 通过schema id 找到其在schmea数组中的索引
     */
    public int getIndexInSchemas(String schemaId)
        throws SemanticAnalyzerException
    {
        for (int i = 0; i < schemas.size(); i++)
        {
            if (schemas.get(i).getId().equals(schemaId))
            {
                return i;
            }
            
            if (schemas.get(i).getName() != null && schemas.get(i).getName().equals(schemaId))
            {
                return i;
            }
            
            if (schemas.get(i).getStreamName() != null && schemas.get(i).getStreamName().equals(schemaId))
            {
                return i;
            }
        }
        
        SemanticAnalyzerException exception = new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NO_STREAM, schemaId);
        LOG.error(ErrorCode.SEMANTICANALYZE_NO_STREAM.getFullMessage(schemaId), exception);
        throw exception;
    }
    
    public List<Schema> getSchemas()
    {
        return schemas;
    }
    
    /**
     * 通过属性名获取属性
     * 可能包含多个
     */
    public List<Column> getAttributeByName(String attrName, Schema schema)
    {
        return BaseAnalyzer.getAttributeByName(attrName, schema, schemas);
    }
    
    /**
     * 通过属性名获取属性
     * 可能包含多个
     * 
     */
    public List<Column> getAttributeByName(String attrName)
    {
        return BaseAnalyzer.getAttributeByName(attrName, schemas);
    }
    
    /**
     * 通过属性名获取该属性所在的schemaid
     * 和getAttributeByName是一一对应的。
     */
    protected List<String> getSchemaNameByAttrName(String attrName)
    {
        Set<String> res = new HashSet<String>();
        for (Schema schema : schemas)
        {
            if (getColumnByNameOrALias(schema.getCols(), attrName).size() > 0)
            {
                res.add(schema.getId());
            }
        }
        return new ArrayList<String>(res);
    }
    
    private List<Column> getColumnByNameOrALias(List<Column> columns, String nameOrAlias)
    {
        List<Column> res = new ArrayList<Column>();
        
        for (Column attr : columns)
        {
            if (!StringUtils.isEmpty(attr.getName()) && attr.getName().equalsIgnoreCase(nameOrAlias))
            {
                res.add(attr);
                continue;
            }
            if (!StringUtils.isEmpty(attr.getAlias()) && attr.getAlias().equalsIgnoreCase(nameOrAlias))
            {
                res.add(attr);
                continue;
            }
        }
        return res;
    }
    
    /**
     * 获取所有流的所有的属性
     * 适用于select *
     * 
     */
    protected List<Column> getAllAttributes()
    {
        List<Column> attrs = new ArrayList<Column>();
        for (Schema schema : schemas)
        {
            attrs.addAll(getAttributes(schema));
        }
        return attrs;
    }
    
    /**
     * 获取一个schema中所有的属性
     * 
     */
    protected List<Column> getAttributes(Schema schema)
    {
        return schema.getCols();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Task createTask(DriverContext driverContext, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public SemanticAnalyzer createAnalyzer()
        throws SemanticAnalyzerException
    {
        return null;
    }
}
