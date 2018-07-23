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

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.SemanticAnalyzerException;
import com.huawei.streaming.cql.semanticanalyzer.analyzecontext.AnalyzeContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * 语义分析基础类
 * 
 */
public abstract class BaseAnalyzer implements SemanticAnalyzer
{
    private static final Logger LOG = LoggerFactory.getLogger(BaseAnalyzer.class);
    
    /**
     * 默认的列名称
     * 当sql没有显式的指定列名称的时候，就需要对列进行自动命名，这个时候用到。
     */
    private static final String DEFAULT_COLUMN_PRIFIX = "x_col_";
    
    /**
     * 对列进行重命名的时候，后缀
     */
    private static final String RENAME_COLUMN_POSTFIX = "_";
    
    private List<Schema> allSchemas = null;
    
    private ParseContext parseContext;
    
    /**
     * <默认构造函数>
     */
    public BaseAnalyzer(ParseContext parseContext)
        throws SemanticAnalyzerException
    {
        this.parseContext = parseContext;
    }
    
    /**
     * {@inheritDoc}
     * Java初始化顺序
     * 父类--静态变量 
     * 父类--静态初始化块 
     * 子类--静态变量 
     * 子类--静态初始化块 
     * 父类--变量 
     * 父类--初始化块 
     * 父类--构造器 
     * 子类--变量 
     * 子类--初始化块 
     * 子类--构造器 
     */
    @Override
    public void init(List<Schema> schemas)
        throws SemanticAnalyzerException
    {
        /*
         * 由于java先初始化父类的一切，再初始化子类的变量和构造器，
         * 所以这里的这些方法的必须放在init方法中，
         * 否则就会发生子类的变量被重新初始化，导致空指针的问题
         */
        createAnalyzeContext();
        getAnalyzeContext().setParseContext(parseContext);
        getAnalyzeContext().validateParseContext();
        
        allSchemas = schemas;
    }
    
    /**
     * 创建analyze语义分析结果保存对象
     */
    protected abstract void createAnalyzeContext();
    
    /**
     * 获取语义分析结果对象
     * 这个方法和analyze的返回值获取的是同一个对象，
     * getAnalyzeContext的接口主要作用在于让当前的抽象类可以自动执行接口方法
     * 不会去解析对象内部的内容是否完整。
     * analyze接口返回值里面包含了语义分析的完整结果。
     */
    protected abstract AnalyzeContext getAnalyzeContext();
    
    public List<Schema> getAllSchemas()
    {
        return allSchemas;
    }
    
    /**
     * 通过名称获取schema
     */
    protected Schema getSchemaByName(String name)
        throws SemanticAnalyzerException
    {
        for (Schema schema : allSchemas)
        {
            if (compareSchema(name, schema))
            {
                return schema;
            }
        }
        SemanticAnalyzerException exception =
            new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NOFOUND_STREAM, name);
        LOG.error("Can't find schema by name.", exception);
        
        throw exception;
    }
    
    /**
     * 通过明名称获取schema
     */
    public static Schema getSchemaByName(String name, List<Schema> schemas)
        throws SemanticAnalyzerException
    {
        for (Schema schema : schemas)
        {
            if (compareSchema(name, schema))
            {
                return schema;
            }
        }
        SemanticAnalyzerException exception =
            new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_NOFOUND_STREAM, name);
        LOG.error("Can't find schema by stream name.", exception);
        
        throw exception;
    }
    
    /**
     * 修改schema中每个属性的shcmea名称
     * 统一schmea的名称和列名称，均为小写。
     * 
     */
    public static void setSchemaNameInAttributes(List<Schema> schemas)
    {
        for (int i = 0; i < schemas.size(); i++)
        {
            Schema s = schemas.get(i);
            for (Column attr : s.getCols())
            {
                attr.setName(attr.getName().toLowerCase(Locale.US));
            }
        }
    }
    
    /**
     * 检查schema是否存在
     */
    protected boolean checkSchemaExists(String name)
    {
        for (Schema schema : allSchemas)
        {
            if (compareSchema(name, schema))
            {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 检查Stream
     * 主要是检查stream中有没有重名的schema名称或者schema别名
     */
    protected void checkSchemas(List<Schema> schemasInCQL)
        throws SemanticAnalyzerException
    {
        Set<String> names = Sets.newHashSet();
        for (Schema s : schemasInCQL)
        {
            /*
             * schema名称不可能为空
             * 但是流的别名可能为空
             */
            if (names.contains(s.getId()))
            {
                SemanticAnalyzerException exception =
                    new SemanticAnalyzerException(ErrorCode.SEMANTICANALYZE_EXISTS_STREAM, s.getId());
                LOG.error("Stream {} already exists.", s.getId(), exception);
                
                throw exception;
            }
        }
    }
    
    /**
     * 对于已经存在的列，重名的列，自动重命名
     */
    protected String renameNewName(Schema schema, String colName)
    {
        
        /*
         * 这个时候，Schema和其中的列一定不为空
         * 因为在进入这个方法之前，肯定已经检查过是否存在同名列了
         * 
         * 并且由于前面已经存在同名列了，所以这个循环检查计数器要从1开始
         */
        int count = 1;
        
        for (Column attr : schema.getCols())
        {
            if (attr.getName().startsWith(colName + RENAME_COLUMN_POSTFIX))
            {
                count++;
            }
        }
        
        return colName + RENAME_COLUMN_POSTFIX + count;
    }
    
    /**
     * 对于通过表达式计算出来的列，需要创建新的列
     */
    protected String createNewName(Schema schema)
    {
        int count = 0;
        if (schema != null && schema.getCols() != null)
        {
            for (Column attr : schema.getCols())
            {
                if (attr.getName().startsWith(DEFAULT_COLUMN_PRIFIX))
                {
                    count++;
                }
            }
        }
        
        return DEFAULT_COLUMN_PRIFIX + count;
    }
    
    /**
     * 获取一个schema中所有的属性
     * 
     */
    public static List<Column> getAttributes(Schema schema)
    {
        return schema.getCols();
    }
    
    /**
     * 获取所有流的所有的属性
     * 适用于select *
     */
    protected static List<Column> getAllAttributes(List<Schema> schemas)
    {
        List<Column> attrs = new ArrayList<Column>();
        for (Schema schema : schemas)
        {
            attrs.addAll(getAttributes(schema));
        }
        return attrs;
    }
    
    private static boolean compareSchema(String name, Schema schema)
    {
        if (!StringUtils.isEmpty(schema.getId()) && schema.getId().equalsIgnoreCase(name))
        {
            return true;
        }
        if (!StringUtils.isEmpty(schema.getName()) && schema.getName().equalsIgnoreCase(name))
        {
            return true;
        }
        if (!StringUtils.isEmpty(schema.getStreamName()) && schema.getStreamName().equalsIgnoreCase(name))
        {
            return true;
        }
        return false;
    }
    
    /**
     * 通过属性名获取属性
     * 可能包含多个
     * 
     */
    public static List<Column> getAttributeByName(String attrName, Schema schema, List<Schema> schemas)
    {
        List<Column> allAttrs = null;
        if (schema == null)
        {
            allAttrs = getAllAttributes(schemas);
        }
        else
        {
            allAttrs = getAttributes(schema);
        }
        
        return getColumnByNameOrALias(allAttrs, attrName);
    }
    
    /**
     * 通过属性名获取属性
     * 可能包含多个
     * 
     */
    public static List<Column> getAttributeByName(String attrName, List<Schema> schemas)
    {
        List<Column> allAttrs = getAllAttributes(schemas);
        return getColumnByNameOrALias(allAttrs, attrName);
    }
    
    private static List<Column> getColumnByNameOrALias(List<Column> columns, String nameOrAlias)
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
    
}
