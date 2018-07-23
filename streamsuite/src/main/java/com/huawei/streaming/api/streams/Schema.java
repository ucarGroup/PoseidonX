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
package com.huawei.streaming.api.streams;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.CQLUtils;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamAsAttribute;
import com.thoughtworks.xstream.annotations.XStreamImplicit;
import com.thoughtworks.xstream.annotations.XStreamOmitField;

/**
 * 流的 Schema信息
 * 不论什么流，都必须包含一个shcema, 
 * schema包含了列名称和列类型
 * 这些Schmea一定是按照顺序来的
 * 
 * 本来在底层有IEventType 作为Schema的
 * 但是由于其属性过多，会导致序列化的时候很多冗余的熟悉感，所以这里就建立了一个新的，
 * 待后面提交任务的时候再进行转换。
 * 
 */
public class Schema implements Serializable
{
    
    private static final long serialVersionUID = 5539423448937278683L;

    private static final Logger LOG = LoggerFactory.getLogger(Schema.class);
    
    /**
     * Schema id，
     * 在CQL解析阶段， 会自动将streamName转为id
     */
    @XStreamAlias("id")
    @XStreamAsAttribute
    private String id;
    
    /**
     * Schema名称
     * 就是Schema的别名，这个别名是允许重复的
     * 但是在CQL中，同一条CQL语句中，别名不能重复。
     * 在IDE中，名称就是Schema的别名
     * 在CQL中，name就是From子句中的别名
     */
    @XStreamAlias("name")
    @XStreamAsAttribute
    //    @XStreamOmitField
    private String name;
    
    /**
     * 流名称
     * 这个流名称在执行器中使用，
     * 在xml API中，a.id这个a既可以是schema的名称
     * 也可以是流名称，因为流名称和连线绑定，一个连线一个schema
     * 
     */
    @XStreamOmitField
    private String streamName;
    
    /**
     * 列信息
     */
    //    @XStreamAlias("attributes")
    @XStreamImplicit(itemFieldName = "attribute")
    private List<Column> cols = new ArrayList<Column>();
    
    /**
     * <默认构造函数>
     */
    public Schema(String id)
    {
        super();
        this.id = id.toLowerCase(Locale.US);
        this.name = id;
    }
    
    /**
     * 根据名称检查该列是否已经添加到了schema中。
     */
    public boolean isAttributeExist(String schemaName)
    {
        if (null == cols)
        {
            return false;
        }
        
        for (Column attr : cols)
        {
            if (attr.getName().equals(schemaName))
            {
                return true;
            }
        }
        return false;
    }
    
    /**
     * 添加列信息到schema中
     */
    public void addCol(Column attribute)
    {
        cols.add(attribute);
    }
    
    public Schema cloneSchema()
    {
        Schema s = new Schema(id);
        s.setName(this.name);
        for (int j = 0; j < cols.size(); j++)
        {
            String colType = cols.get(j).getType();
            String colName = cols.get(j).getName();
            Class< ? > type = getColumnTypeClass(colType);
            if (type == null)
            {
                return null;
            }
            s.addCol(new Column(colName, type));
        }
        return s;
    }
    
    private Class< ? > getColumnTypeClass(String colTyple)
    {
        if (colTyple == null)
        {
            return null;
        }
        
        try
        {
            return Class.forName(colTyple, true, CQLUtils.getClassLoader());
        }
        catch (ClassNotFoundException e)
        {
            LOG.error("failed to get attribute type {}", colTyple, e);
            return null;
        }
    }
    
    public List<Column> getCols()
    {
        return cols;
    }
    
    public void setCols(List<Column> cols)
    {
        this.cols = cols;
    }
    
    public String getId()
    {
        return id;
    }
    
    public void setId(String id)
    {
        this.id = id.toLowerCase(Locale.US);
    }
    
    public String getName()
    {
        return name;
    }
    
    public void setName(String name)
    {
        this.name = name;
    }
    
    public String getStreamName()
    {
        return streamName;
    }
    
    public void setStreamName(String streamName)
    {
        this.streamName = streamName;
    }
}
