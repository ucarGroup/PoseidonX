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
package com.huawei.streaming.cql.mapping;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.huawei.streaming.datasource.IDataSource;
import com.huawei.streaming.datasource.RDBDataSource;
import com.huawei.streaming.operator.IInputStreamOperator;
import com.huawei.streaming.operator.IOutputStreamOperator;
import com.huawei.streaming.operator.inputstream.HeadStreamSourceOp;
import com.huawei.streaming.operator.inputstream.KafkaSourceOp;
import com.huawei.streaming.operator.inputstream.TCPClientInputOperator;
import com.huawei.streaming.operator.outputstream.*;
import com.huawei.streaming.serde.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * CQL内置的输入流和输出流简写映射
 * <p/>
 * 不区分大小写
 *
 */
public enum SimpleLexer
{

    INPUT()
        {
            private final Map<String, String> INPUTOPERATOR_MAPPING = Maps.newConcurrentMap();

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getSimpleName(String fullName)
            {
               return baseGetSimpleName(fullName, INPUTOPERATOR_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getFullName(String simpleName)
            {
                return baseGetFullName(simpleName, INPUTOPERATOR_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean register(String simpleName, Class<?> clazz)
            {
                return baseRegister(simpleName, clazz, INPUTOPERATOR_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean unRegister(String simpleName)
            {
                return baseUnRegister(simpleName, INPUTOPERATOR_MAPPING);
            }
        },

    OUTPUT()
        {
            private final Map<String, String> OUTPUTOPERTOR_MAPPING = Maps.newConcurrentMap();

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getSimpleName(String fullName)
            {
                return baseGetSimpleName(fullName, OUTPUTOPERTOR_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getFullName(String simpleName)
            {
                return baseGetFullName(simpleName, OUTPUTOPERTOR_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean register(String simpleName, Class<?> clazz)
            {
                return baseRegister(simpleName, clazz, OUTPUTOPERTOR_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean unRegister(String simpleName)
            {
                return baseUnRegister(simpleName, OUTPUTOPERTOR_MAPPING);
            }
        },

    SERDE()
        {
            private final Map<String, String> SERDE_MAPPING = Maps.newConcurrentMap();

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getSimpleName(String fullName)
            {
                return baseGetSimpleName(fullName, SERDE_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getFullName(String simpleName)
            {
                return baseGetFullName(simpleName, SERDE_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean register(String simpleName, Class<?> clazz)
            {
                return baseRegister(simpleName, clazz, SERDE_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean unRegister(String simpleName)
            {
                return baseUnRegister(simpleName, SERDE_MAPPING);
            }
        },
    DATASOURCE()
        {
            private final Map<String, String> DATASOURCE_MAPPING = Maps.newConcurrentMap();

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getSimpleName(String fullName)
            {
                return baseGetSimpleName(fullName, DATASOURCE_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            public synchronized String getFullName(String simpleName)
            {
               return baseGetFullName(simpleName, DATASOURCE_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean register(String simpleName, Class<?> clazz)
            {
                return baseRegister(simpleName, clazz, DATASOURCE_MAPPING);
            }

            /**
             * {@inheritDoc}
             */
            @Override
            protected synchronized boolean unRegister(String simpleName)
            {
                return baseUnRegister(simpleName, DATASOURCE_MAPPING);
            }
        };

    private static final Logger LOG = LoggerFactory.getLogger(SimpleLexer.class);

    /**
     * 通过全称获取简称，如果不存在，返回null
     */
    public abstract String getSimpleName(String fullName);

    /**
     * 获取全称，如果不存在，返回null
     */
    public abstract String getFullName(String simpleName);

    /**
     * 注册简称，
     * 注意，注册可能会导致系统注册变量丢失，即使后面移除了注册，也不会恢复。
     */
    protected abstract boolean register(String simpleName, Class<?> clazz);

    /**
     * 移除注册，如果返回false，移除失败，说明简称不存在
     */
    protected abstract boolean unRegister(String simpleName);

    static
    {
        //序列化反序列化类
        registerSerDe("SimpleSerDe", SimpleSerDe.class);
        registerSerDe("KeyValueSerDe", KeyValueSerDe.class);
        registerSerDe("CsvSerDe", CSVSerDe.class);
        registerSerDe("BinarySerDe", BinarySerDe.class);

        registerInputOperator("KafkaInput", KafkaSourceOp.class);
        registerInputOperator("TCPClientInput", TCPClientInputOperator.class);
        registerInputOperator("RandomGen", HeadStreamSourceOp.class);
        registerOutputOperator("KafkaOutput", KafkaFunctionOp.class);
        registerOutputOperator("TCPClientOutput", TCPSenderFuncOp.class);
        registerOutputOperator("ConsoleOutput", ConsolePrintOp.class);

        registerDataSource("RDBDataSource", RDBDataSource.class);
    }

    /**
     * 注册序列化类/反序列化类的简称
     * 由于没有办法区分哪些是系统注册的，哪些是用户注册的，所以默认后注册的覆盖先注册的。
     * 由泛型来对注册类进行限制，方式注册错误类，这样就可以在编译期发现问题。
     */
    public static void registerSerDe(String name, Class<? extends StreamSerDe> clazz)
    {
        SERDE.register(name, clazz);
    }

    /**
     * 注册各类算子简称，这里的算子，一定是包含了可执行接口的算子
     * 由于没有办法区分哪些是系统注册的，哪些是用户注册的，所以默认后注册的覆盖先注册的。
     * 由泛型来对注册类进行限制，方式注册错误类，这样就可以在编译期发现问题。
     */
    public static void registerInputOperator(String name, Class<? extends IInputStreamOperator> clazz)
    {
        INPUT.register(name, clazz);
    }

    /**
     * 注册各类算子简称，这里的算子，一定是包含了可执行接口的算子
     * 由于没有办法区分哪些是系统注册的，哪些是用户注册的，所以默认后注册的覆盖先注册的。
     * 由泛型来对注册类进行限制，方式注册错误类，这样就可以在编译期发现问题。
     */
    public static void registerOutputOperator(String name, Class<? extends IOutputStreamOperator> clazz)
    {
        OUTPUT.register(name, clazz);
    }

    /**
     * 注册数据源简称，这里的数据源，一定是包含了可执行接口的数据源接口实现
     * 由于没有办法区分哪些是系统注册的，哪些是用户注册的，所以默认后注册的覆盖先注册的。
     * 由泛型来对注册类进行限制，方式注册错误类，这样就可以在编译期发现问题。
     */
    public static void registerDataSource(String name, Class<? extends IDataSource> clazz)
    {
        DATASOURCE.register(name, clazz);
    }

    /**
     * 移除语法简称
     *
     */
    public static boolean unRegisterSerDe(String name)
    {
        return SERDE.unRegister(name);
    }

    /**
     * 移除语法简称
     *
     */
    public static boolean unRegisterInput(String name)
    {
        return INPUT.unRegister(name);
    }

    /**
     * 移除语法简称
     *
     */
    public static boolean unRegisterOutput(String name)
    {
        return OUTPUT.unRegister(name);
    }

    /**
     * 移除语法简称
     *
     */
    public static boolean unRegisterDataSource(String name)
    {
       return DATASOURCE.unRegister(name);
    }

    //加入同步防止多线程同时添加或者删除导致的错误，虽然说map本身是同步的，但是方法并不同步
    private synchronized static boolean baseUnRegister(String name, Map<String, String> mapping)
    {
        if (Strings.isNullOrEmpty(name))
        {
            LOG.warn("Failed to unRegister simple lexer mapping, name is null.");
            return false;
        }

        //由于可能移除系统注册的简称，所以这里级别是Warn
        LOG.warn("UnRegister '{}' from simple lexers.", name);
        if (mapping.containsKey(name))
        {
            mapping.remove(name);
        }
        return true;
    }

    private synchronized static boolean baseRegister(String simpleName, Class<?> clazz, Map<String, String> mapping)
    {
        if (Strings.isNullOrEmpty(simpleName) || clazz == null)
        {
            LOG.warn("Failed to register simple lexer mapping, name or class is null.");
            return false;
        }

        LOG.info("register simple lexer {}, class {}.", simpleName, clazz);
        if (mapping.containsKey(simpleName))
        {
            LOG.warn("Exists simple lexxer {} will be repalced.", simpleName);
        }
        mapping.put(simpleName, clazz.getName());
        return true;
    }

    private synchronized static String baseGetSimpleName(String fullName, Map<String, String> mapping)
    {
        if (fullName == null)
        {
            return null;
        }

        for (Map.Entry<String, String> et : mapping.entrySet())
        {
            if (fullName.equals(et.getValue()))
            {
                return et.getKey();
            }
        }

        return null;
    }

    private synchronized static String baseGetFullName(String simpleName, Map<String, String> mapping)
    {
        if (Strings.isNullOrEmpty(simpleName))
        {
            return null;
        }

        for (Map.Entry<String, String> et : mapping.entrySet())
        {
            if (simpleName.equalsIgnoreCase(et.getKey()))
            {
                return et.getValue();
            }
        }

        return null;
    }
}
