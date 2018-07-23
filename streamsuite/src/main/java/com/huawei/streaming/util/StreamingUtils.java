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

package com.huawei.streaming.util;

import com.huawei.streaming.event.TupleEventType;
import com.huawei.streaming.exception.StreamingRuntimeException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.commons.lang.ArrayUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 系统工具类，提供基本的工具函数
 */
public class StreamingUtils
{
    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(StreamingUtils.class);

    /**
     * 空字符串,分割字符串时用到
     */
    private static final String EMPTY_STR = "";

    /**
     * 关闭连接
     */
    public static void close(AutoCloseable... statements)
    {
        for (AutoCloseable sts : statements)
        {
            if (sts == null)
            {
                continue;
            }

            try
            {
                sts.close();
            }
            catch (Exception e)
            {
                //忽略关闭异常，不打印堆栈
                LOG.warn("Failed to close statement.");
            }
        }
    }

    /**
     *
     *  分割字符串
     *
     *  按照分割符分割字符串，以字符串数组返回，比jdk提供的String.split()方法高效
     *  10万次分割的时间对比： String.split() 250毫秒； split 45毫秒
     *
     */
    public static String[] split(String strOriginal, String strSeparatorChars)
    {
        return splitByWholeSeparator(strOriginal, strSeparatorChars, -1, false);
    }

    /**
     *
     *  分割字符串
     *
     *  按照分割符分割字符串，以字符串数组返回，比jdk提供的String.split()方法高效
     *  10万次分割的时间对比： String.split() 250毫秒； split 45毫秒
     *
     */
    public static String[] split(String strOriginal, String strSeparatorChars, int iMax)
    {
        return splitByWholeSeparator(strOriginal, strSeparatorChars, iMax, false);
    }

    /**
     *
     * 按照分割符分割字符串，以字符串数组返回    
     *
     * 按照分割符分割字符串，以字符串数组返回，比jdk提供的String.split()方法高效
     * 10万次分割的时间对比： String.split() 250毫秒； split 45毫秒
     *
     */
    public static String[] splitByWholeSeparator(String strOriginal, String strSeparatorChars, int iMax,
        boolean blPreserveAllTokens)
    {
        //源字符串不合法
        if (strOriginal == null)
        {
            LOG.warn("strOriginal is null!");
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        //源字符串的长度
        int iLength = strOriginal.length();

        if (iLength == 0)
        {
            LOG.warn("strOriginal is empty!");
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        //分割符不合法
        if ((strSeparatorChars == null) || (EMPTY_STR.equals(strSeparatorChars)))
        {
            LOG.warn("strSeparatorChars is invalid!");
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }

        //分割符的长度
        int iSeparatorLength = strSeparatorChars.length();

        //保存分割后字符串的list
        ArrayList<String> substrList = new ArrayList<String>();

        //分割的字符串数组长度
        int iNumberOfSubstrings = 0;

        //起始位置
        int iBegin = 0;

        //结束位置
        int iEnd = 0;

        while (iEnd < iLength)
        {
            //获取分割符的索引
            iEnd = strOriginal.indexOf(strSeparatorChars, iBegin);

            //能找到分割符
            if (iEnd > -1)
            {
                if (iEnd > iBegin)
                {
                    iNumberOfSubstrings += 1;

                    //如果已经达到分割字符串的最大长度，将剩余字符串放在最后，分割结束
                    if (iNumberOfSubstrings == iMax)
                    {
                        iEnd = iLength;

                        substrList.add(strOriginal.substring(iBegin));
                    }
                    else
                    {
                        //保存分割符之前的字符串
                        substrList.add(strOriginal.substring(iBegin, iEnd));

                        //设置下次循环查找的起始位置 
                        iBegin = iEnd + iSeparatorLength;
                    }
                }
                else
                {
                    //如果允许分割字符串连续出现
                    if (blPreserveAllTokens)
                    {
                        iNumberOfSubstrings += 1;

                        //如果已经达到分割字符串的最大长度，将剩余字符串放在最后，分割结束
                        if (iNumberOfSubstrings == iMax)
                        {
                            iEnd = iLength;

                            substrList.add(strOriginal.substring(iBegin));
                        }
                        //将分割符替换为空字符串
                        else
                        {
                            substrList.add(EMPTY_STR);
                        }
                    }

                    //设置下次循环查找的起始位置 
                    iBegin = iEnd + iSeparatorLength;
                }
            }
            //已找不到分割符，表示最后一个分割符已经处理完
            else
            {
                //保存最后一个分割符后面的字符串
                substrList.add(strOriginal.substring(iBegin));

                iEnd = iLength;
            }
        }

        //以字符串数组的形式返回
        return (String[])substrList.toArray(new String[substrList.size()]);
    }


    public static String serializeSchemas(List<TupleEventType> schemas) {
        return serialize(schemas);
    }

    public static List<TupleEventType> deSerializeSchemas(String str) {
        return deSerialize(str);
    }

    public static String serializeSchema(TupleEventType schema) {
        return serialize(schema);
    }

    public static TupleEventType deSerializeSchema(String str) {
        return deSerialize(str);
    }

    private static String serialize(Object schemas) {
        if (schemas == null) {
            return null;
        }

        String str = null;
        ByteArrayOutputStream baops = null;
        ObjectOutputStream oos = null;

        try {
            baops = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baops);
            oos.writeObject(schemas);
            byte[] bytes = baops.toByteArray();
            str = Base64.encodeBase64String(bytes);
        } catch (IOException e) {
            throw new StreamingRuntimeException("Can not serialize TupleEventType to String");
        } finally {
            close(oos, baops);
        }
        return str;
    }

    private static <T extends Serializable> T deSerialize(String str) {
        if (str == null || str.isEmpty()) {
            return null;
        }

        byte[] bytes = Base64.decodeBase64(str);
        ObjectInputStream ois = null;
        T obj = null;

        try {
            ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
            obj = (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new StreamingRuntimeException("Can not deserialize string to TupleEventType");
        } catch (IOException e) {
            throw new StreamingRuntimeException("Can not deserialize string to TupleEventType");
        } finally {
            close(ois);
        }
        return obj;
    }
}
