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

package com.huawei.streaming.event;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.lang.StringUtils;

/**
 * 
 * 事件类型
 * <功能详细描述>
 * 
 */
public class TupleEventType implements IEventType
{
    /**
     * 序列化id
     */
    private static final long serialVersionUID = 4219204970826750626L;
    
    private String name;
    
    private Attribute[] schema;
    
    private String[] attNames;
    
    private Class< ? >[] attTypes;
    
    private HashMap<String, Integer> attid;
    
    private int size;
    
    /**
     * <默认构造函数>
     *@param sn 流名称
     *@param schema 事件属性集
     */
    public TupleEventType(String sn, List<Attribute> schema)
    {
        newSchema(sn, schema);
    }
    
    /**
     * <默认构造函数>
     *@param sn 流名称
     *@param schema 事件属性
     */
    public TupleEventType(String sn, Attribute... schema)
    {
        List<Attribute> att = new ArrayList<Attribute>();
        for (Attribute a : schema)
        {
            att.add(a);
        }
        newSchema(sn, att);
    }
    
    private void newSchema(String sn, List<Attribute> atts)
    {
        if (StringUtils.isEmpty(sn))
        {
            throw new RuntimeException("The schema name is empty.");
        }
        if (null == atts || atts.size() == 0)
        {
            throw new RuntimeException("The schema is null.");
        }
        
        this.name = sn;
        if (null == this.schema)
        {
            this.schema = new Attribute[atts.size()];
        }
        /**
         * 判断输入的schema是否合法：属性名称是否唯一
         */
        String n = null;
        int i = 0;
        attNames = new String[atts.size()];
        attTypes = new Class[atts.size()];
        attid = new HashMap<String, Integer>();
        
        for (Attribute a : atts)
        {
            n = a.getAttName();
            if (!attid.containsKey(n))
            {
                attid.put(n, i);
                attNames[i] = n;
                attTypes[i] = a.getAttDataType();
                this.schema[i] = a;
                i++;
            }
            else
            {
                throw new RuntimeException("Duplicated attribute name. name=" + n);
            }
        }
        
        size = atts.size();
    }
    
    public String[] getAllAttributeNames()
    {
        return this.attNames;
    }
    
    public Class< ? >[] getAllAttributeTypes()
    {
        return this.attTypes;
    }
    
    /**
     * {@inheritDoc}
     */
    public int getAttributeIndex(String attribute)
    {
        if (StringUtils.isEmpty(attribute))
        {
            return -1;
        }
        if (!attid.containsKey(attribute))
        {
            return -1;
        }
        
        return attid.get(attribute);
    }
    
    /**
     * {@inheritDoc}
     */
    public String getAttributeName(int index)
    {
        if (index < 0 || index >= attNames.length)
        {
            throw new RuntimeException("The index of attribute is out of range. index=" + index);
        }
        return attNames[index];
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public Attribute getAttribute(String attName)
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String getEventTypeName()
    {
        return name;
    }
    
    @Override
    public Attribute[] getAllAttributes()
    {
        return schema;//.toArray(new Attribute[schema.size()]);
    }
    
    public int getSize()
    {
        return this.size;
    }
    
    /**
     * {@inheritDoc}
     */
    public String toString()
    {
        return name;
    }
    
}
