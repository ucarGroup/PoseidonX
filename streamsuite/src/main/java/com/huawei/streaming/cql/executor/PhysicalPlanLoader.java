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

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.PhysicalPlan;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.api.opereators.*;
import com.huawei.streaming.api.opereators.serdes.BinarySerDeAPI;
import com.huawei.streaming.api.opereators.serdes.CSVSerDeAPI;
import com.huawei.streaming.api.opereators.serdes.KeyValueSerDeAPI;
import com.huawei.streaming.api.opereators.serdes.SimpleSerDeAPI;
import com.huawei.streaming.api.opereators.serdes.UserDefinedSerDeAPI;
import com.huawei.streaming.api.streams.Column;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.exception.ErrorCode;
import com.thoughtworks.xstream.XStream;
import com.thoughtworks.xstream.converters.reflection.SunUnsafeReflectionProvider;
import com.thoughtworks.xstream.core.ClassLoaderReference;
import com.thoughtworks.xstream.io.ReaderWrapper;
import com.thoughtworks.xstream.io.StreamException;
import com.thoughtworks.xstream.io.xml.DomDriver;
import com.thoughtworks.xstream.mapper.DefaultMapper;
import com.thoughtworks.xstream.mapper.MapperWrapper;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Map;

/**
 * 加载物理执行计划
 *
 */
public class PhysicalPlanLoader
{
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalPlanLoader.class);

    /**
     * xml 文件的编码格式
     */
    private static final String XML_CHARSET = "utf-8";

    private static Map< String, Class< ? > > ALIAS_MAPPING = Maps.newConcurrentMap();

    static
    {
        setAlias();
    }

    /**
     * 从指定路径加载执行计划
     * <p/>
     * 由于xml中可能存在一些额外的空格或者tab之类的符号，所以要通过trim把这些内容移除掉
     *
     */
    public static PhysicalPlan load(String path) throws ExecutorException
    {
        XStream xstream = new XStream(new SunUnsafeReflectionProvider(), new DomDriver())
        {
            protected MapperWrapper wrapMapper(MapperWrapper next)
            {
                return new MapperWrapper(next)
                {
                    public boolean shouldSerializeMember(Class definedIn, String fieldName)
                    {
                        return definedIn != Object.class ? super.shouldSerializeMember(definedIn, fieldName) : false;
                    }

                };
            }

        };

        setAlias(xstream);
        xstream.registerConverter(new MapConverter(new DefaultMapper(new ClassLoaderReference(PhysicalPlanLoader.class.getClassLoader()))));
        xstream.autodetectAnnotations(true);
        ReaderWrapper wrapper = null;
        InputStream in = null;
        PhysicalPlan plan = null;
        DomDriver driver = new DomDriver(XML_CHARSET);
        try
        {
            in = new FileInputStream(new File(path));
            wrapper = new ReaderWrapper(driver.createReader(in))
            {
                public String getValue()
                {
                    return super.getValue().trim();
                }
            };
            plan = (PhysicalPlan)xstream.unmarshal(wrapper);
        }
        catch (FileNotFoundException e)
        {
            ExecutorException exception = new ExecutorException(e, ErrorCode.TOP_PHYSICPLAN_NOT_EXISTS, path);
            LOG.error("PhysicalPlan file not found.");
            throw exception;
        }
        catch (StreamException e1)
        {
            ExecutorException exception = new ExecutorException(e1, ErrorCode.TOP_PHYSICPLAN_ERROR_CONTEXT, path);
            LOG.error(ErrorCode.TOP_PHYSICPLAN_ERROR_CONTEXT.getFullMessage(path), e1);
            throw exception;
        }
        finally
        {
            IOUtils.closeQuietly(in);
        }

        return plan;
    }

    private static void setAlias()
    {
        ALIAS_MAPPING.put("Operator", Operator.class);

        setFunctionStreamAlias();
        setInputOutputStreamAlias();
        setSerDeAlias();
        setDataSourceAlias();
        ALIAS_MAPPING.put("Definitions", PhysicalPlan.class);
        ALIAS_MAPPING.put("Application", Application.class);
        ALIAS_MAPPING.put("Transition", OperatorTransition.class);

        ALIAS_MAPPING.put("JoinType", JoinType.class);

        ALIAS_MAPPING.put("Schema", Schema.class);
        ALIAS_MAPPING.put("UserFunction", UserFunction.class);
    }

    /**
     * 设置Xstream别名
     *
     */
    public static void setAlias(XStream xstream)
    {
        for (Map.Entry< String, Class< ? > > et : ALIAS_MAPPING.entrySet())
        {
            xstream.alias(et.getKey(), et.getValue());
        }
        xstream.addImplicitCollection(Schema.class, "cols", "attribute", Column.class);
    }

    public static void registerPhysicalPlanAlias(String alias, Class< ? > clazz)
    {
        if (Strings.isNullOrEmpty(alias))
        {
            LOG.warn("Unable to register alias to physic plan. alias is null.");
            return;
        }

        LOG.info("Register alias to physic plan.");
        ALIAS_MAPPING.put(alias, clazz);
    }

    public static void unRegisterPhysicalPlanAlias(String alias)
    {
        if (Strings.isNullOrEmpty(alias))
        {
            LOG.warn("Unable to unRegister alias to physic plan. alias is null.");
            return;
        }

        LOG.info("unRegister alias to physic plan.");
        if (ALIAS_MAPPING.containsKey(alias))
        {
            ALIAS_MAPPING.remove(alias);
        }
    }


    private static void setDataSourceAlias()
    {
        ALIAS_MAPPING.put("BaseDataSource", BaseDataSourceOperator.class);
        ALIAS_MAPPING.put("DataSource", DataSourceOperator.class);
        ALIAS_MAPPING.put("RDBDataSource", RDBDataSourceOperator.class);
        ALIAS_MAPPING.put("RedisStringDataSource", RedisStringDataSourceOperator.class);
    }

    private static void setSerDeAlias()
    {
        ALIAS_MAPPING.put("SimpleSerDe", SimpleSerDeAPI.class);
        ALIAS_MAPPING.put("KeyValueSerDe", KeyValueSerDeAPI.class);
        ALIAS_MAPPING.put("CSVSerDe", CSVSerDeAPI.class);
        ALIAS_MAPPING.put("BinarySerDe", BinarySerDeAPI.class);
        ALIAS_MAPPING.put("UserDefinedSerDe", UserDefinedSerDeAPI.class);
    }

    private static void setInputOutputStreamAlias()
    {
        ALIAS_MAPPING.put("InputOperator", InputStreamOperator.class);
        ALIAS_MAPPING.put("OutputOperator", OutputStreamOperator.class);
        ALIAS_MAPPING.put("TCPInput", TCPClientInputOperator.class);
        ALIAS_MAPPING.put("TCPOutput", TCPClientOutputOperator.class);
        ALIAS_MAPPING.put("RandomGenInput", RandomGenInputOperator.class);
        ALIAS_MAPPING.put("ConsoleOutput", ConsoleOutputOperator.class);
        ALIAS_MAPPING.put("KafkaInput", KafkaInputOperator.class);
        ALIAS_MAPPING.put("KafkaOutput", KafkaOutputOperator.class);
        ALIAS_MAPPING.put("FlexQInput", FlexQInputOperator.class);
        ALIAS_MAPPING.put("FlexQOutput", FlexQOutputOperator.class);
        ALIAS_MAPPING.put("RedisStringOutput", RedisStringOutputOperator.class);
    }

    private static void setFunctionStreamAlias()
    {
        ALIAS_MAPPING.put("FunctionStream", FunctionStreamOperator.class);
        ALIAS_MAPPING.put("Function", InnerFunctionOperator.class);
        ALIAS_MAPPING.put("BasicAggregator", BasicAggFunctionOperator.class);
        ALIAS_MAPPING.put("Aggregator", AggregateOperator.class);
        ALIAS_MAPPING.put("Join", JoinFunctionOperator.class);
        ALIAS_MAPPING.put("Filter", FilterOperator.class);
        ALIAS_MAPPING.put("Functor", FunctorOperator.class);
        ALIAS_MAPPING.put("Combiner", CombineOperator.class);
        ALIAS_MAPPING.put("Union", UnionOperator.class);
        ALIAS_MAPPING.put("Splitter", SplitterOperator.class);
        ALIAS_MAPPING.put("SubSplitter", SplitterSubContext.class);
    }

}
