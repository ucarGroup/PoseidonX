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

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.PhysicalPlan;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.core.ClassLoaderReference;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.mapper.DefaultMapper;

/**
 * 序列化物理执行计划
 * 
 */
public class PhysicalPlanWriter
{
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalPlanLoader.class);
    
    /**
     * xml 文件的编码格式
     */
    private static final String XML_CHARSET = "utf-8";
    
    /**
     * 使用XStream的时候，如果用了DOMDriver，是不会输出xml的申明头文字的，所以在序列化的时候要手工添加
     */
    private static final String XML_HEADER = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n";
    
    /**
     * 将应用程序写入到指定文件
     */
    public static void write(Application app, String file)
    {
        PhysicalPlan plan = new PhysicalPlan();
        plan.setApploication(app);
        write(plan, file);
    }
    
    /**
     * 将执行计划写入到指定文件
     */
    public static void write(PhysicalPlan plan, String file)
    {
        String s = createStringPlan(plan);
        try
        {
            FileUtils.writeStringToFile(new File(file), XML_HEADER + s, XML_CHARSET);
        }
        catch (IOException e)
        {
            LOG.error("failed to write physical plan to file {} for io error.", file);
        }
    }
    
    /**
     * 将执行计划序列化成字符串
     */
    public static String createStringPlan(PhysicalPlan plan)
    {
        XStream xstream = new XStream(new DomDriver(XML_CHARSET));
        xstream.autodetectAnnotations(true);
        PhysicalPlanLoader.setAlias(xstream);
        xstream.registerConverter(new MapConverter(new DefaultMapper(new ClassLoaderReference(PhysicalPlanWriter.class.getClassLoader()))));
        return xstream.toXML(plan);
    }
    
    /**
     * 将应用程序序列化为执行计划字符串
     */
    public static String createStringPlan(Application app)
    {
        PhysicalPlan plan = new PhysicalPlan();
        plan.setApploication(app);
        return createStringPlan(plan);
    }
    
}
