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

package com.huawei.streaming.cql.executor;

import java.util.Map.Entry;
import java.util.TreeMap;

import com.thoughtworks.xstream.converters.MarshallingContext;
import com.thoughtworks.xstream.converters.UnmarshallingContext;
import com.thoughtworks.xstream.converters.collections.AbstractCollectionConverter;
import com.thoughtworks.xstream.io.ExtendedHierarchicalStreamWriterHelper;
import com.thoughtworks.xstream.io.HierarchicalStreamReader;
import com.thoughtworks.xstream.io.HierarchicalStreamWriter;
import com.thoughtworks.xstream.mapper.Mapper;

/**
 * 定制XStream的自己的转换器，将map转化为kv形式
 *
 */
public class MapConverter extends AbstractCollectionConverter
{
    /**
     * <默认构造函数>
     *
     */
    public MapConverter(Mapper mapper)
    {
        super(mapper);
    }
    
    /**
     * {@inheritDoc}
     */
    public boolean canConvert(Class type)
    {
        return TreeMap.class == type;
    }
    
    /**
     * {@inheritDoc}
     */
    public void marshal(Object source, HierarchicalStreamWriter writer, MarshallingContext context)
    {
        TreeMap<String, String> map = (TreeMap)source;
        
        for (Entry<String, String> entry : map.entrySet())
        {
            ExtendedHierarchicalStreamWriterHelper.startNode(writer, "property", Entry.class);
            
            writer.addAttribute("key", entry.getKey());
            writer.addAttribute("value", entry.getValue());
            writer.endNode();
        }
    }
    
    /**
     * {@inheritDoc}
     */
    public Object unmarshal(HierarchicalStreamReader reader, UnmarshallingContext context)
    {
        TreeMap map = (TreeMap)createCollection(context.getRequiredType());
        populateMap(reader, map);
        return map;
    }
    
    /**
     * map的键值解析类
     *
     */
    protected void populateMap(HierarchicalStreamReader reader, TreeMap map)
    {
        while (reader.hasMoreChildren())
        {
            reader.moveDown();
            Object key = reader.getAttribute("key");
            Object value = reader.getAttribute("value");
            map.put(key, value);
            reader.moveUp();
        }
    }
}
