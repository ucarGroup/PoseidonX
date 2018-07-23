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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.cql.CQLUtils;
import com.huawei.streaming.cql.executor.expressioncreater.PropertyValueExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * propertyValue表达式的描述信息
 * 
 */
@ExpressionCreatorAnnotation(PropertyValueExpressionCreator.class)
public class PropertyValueExpressionDesc implements ExpressionDescribe
{
    /**
     *  属性
     */
    private String property;
    
    /**
     * 属性对应的数据类型
     */
    private Class< ? > type;
    
    /**
     * 流名称
     */
    private String schemaId;
    
    private int indexInSchemas;
    
    /**
     * <默认构造函数>
     */
    public PropertyValueExpressionDesc(String property, Class< ? > type, String schemaid, int indexInSchemas)
    {
        super();
        this.property = property;
        this.type = type;
        this.schemaId = schemaid;
        this.indexInSchemas = indexInSchemas;
    }
    
    /**
     * <默认构造函数>
     */
    public PropertyValueExpressionDesc(Column attr, String schemaid, int indexInSchemas)
    {
        this.property = attr.getName();
        try
        {
            this.type = Class.forName(attr.getType(), true, CQLUtils.getClassLoader());
        }
        catch (ClassNotFoundException e)
        {
            throw new StreamingRuntimeException(e);
        }
        this.schemaId = schemaid;
        this.indexInSchemas = indexInSchemas;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return schemaId + "." + property;
    }
    
    public String getProperty()
    {
        return property;
    }
    
    public void setProperty(String property)
    {
        this.property = property;
    }
    
    public Class< ? > getType()
    {
        return type;
    }
    
    public void setType(Class< ? > type)
    {
        this.type = type;
    }
    
    public String getSchemaId()
    {
        return schemaId;
    }
    
    public void setSchemaId(String schemaId)
    {
        this.schemaId = schemaId;
    }
    
    public int getIndexInSchemas()
    {
        return indexInSchemas;
    }
    
    public void setIndexInSchemas(int indexInSchemas)
    {
        this.indexInSchemas = indexInSchemas;
    }
}
