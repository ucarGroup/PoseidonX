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

package com.huawei.streaming.serde;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Locale;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.operator.inputstream.Bytes;
import com.huawei.streaming.operator.inputstream.Input;
import com.huawei.streaming.util.StreamingDataType;
import com.huawei.streaming.util.StreamingUtils;
import com.huawei.streaming.util.datatype.DataTypeParser;
import com.huawei.streaming.util.datatype.DateParser;
import com.huawei.streaming.util.datatype.DecimalParser;
import com.huawei.streaming.util.datatype.TimeParser;
import com.huawei.streaming.util.datatype.TimestampParser;

/**
 * 二进制包数据读取
 * <p/>
 * 各类字段默认长度：
 * int length : 4
 * long length : 8
 * float length : 4
 * double length : 8
 * boolean length : 1
 * decimal length : 4~316
 * string length : 0~~ 空字符串长度为0
 * Time length : 8(String类型)或者8(Long类型)
 * Date length : 10(String类型)或者8(Long类型)
 * TimeStamp : 23(String类型)或者8(Long类型)
 *
 */
public class BinarySerDe extends BaseSerDe
{
    private static final long serialVersionUID = -4006642143058239612L;

    private static final Logger LOG = LoggerFactory.getLogger(BinarySerDe.class);

    private static final String STRING_VIEW = "STRING";

    private static final String LONG_VIEW = "LONG";

    private static final String DECIMAL_VIEW = "DECIMAL";

    /**
     * byte在数组默认值
     */
    private static final char NULL_BYTE = '\u0000';

    private List< Object[] > nullResults = Lists.newArrayList();

    private Integer[] binaryLengthArr;

    private int allLength;

    private String attributesLengths;

    private boolean isLongTimeView = true;

    /**
     * decimal类型默认使用原生decimal方式进行binary操作
     */
    private boolean isDecimalNativeView = true;

    private DataTypeParser dateParser;
    private DataTypeParser timeParser;
    private DataTypeParser timestampParser;
    private DataTypeParser decimalParser;

    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
     throws StreamingException
    {
        super.setConfig(conf);
        attributesLengths = getConfig().getStringValue(StreamingConfig.SERDE_BINARYSERDE_ATTRIBUTESLENGTH);
        recognizeTimeType();
        recognizeDecimalType();
        //setconfig发生比算子的setconfig早，所以无法通过获取schema来检查参数是否有效
    }


    /**
     * 初始化方法
     * 序列化对象一定是只初始化一次，
     * 之后每次调用，都执行执行一次序列化。
     *
     */
    @Override
    public void initialize()
     throws StreamSerDeException
    {
        super.initialize();

        try
        {
            dateParser = new DateParser(this.getConfig());
            timeParser = new TimeParser(this.getConfig());
            timestampParser = new TimestampParser(this.getConfig());
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to initialize timestampParser.");
            throw new StreamSerDeException("Failed to initialize timestampParser.", e);
        }
        decimalParser = new DecimalParser();
        String[] lengthArr = StreamingUtils.split(attributesLengths, ",");
        int schemaColumnsLength = getSchema().getAllAttributes().length;
        if (lengthArr.length != schemaColumnsLength)
        {
            LOG.error("Attributes length {} is not equals with attributes size {} .",
             lengthArr.length,
             schemaColumnsLength);
            throw new StreamSerDeException("Attributes length " + lengthArr.length
             + " is not equals with attributes size " + schemaColumnsLength + " .");
        }
        formatAttributesLength(lengthArr);
        try
        {
            validateByteSize();
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to validate byte value length.");
            throw new StreamSerDeException("Failed to validate byte value length.");
        }
    }

    /**
     * 各个字段长度的检查
     * 只能检查出最小长度
     * 并且无法在编译时候检查
     */
    private void validateByteSize() throws StreamingException, StreamSerDeException
    {
        String invalidMessage = "Invalid binary value length. %s type min length is %s";
        Class< ? >[] dataTypes = getSchema().getAllAttributeTypes();
        for (int i = 0; i < dataTypes.length; i++)
        {
            int length = binaryLengthArr[i];
            StreamingDataType dataType = StreamingDataType.getStreamingDataType(dataTypes[i]);
            switch (dataType)
            {
                case INT:
                    validateIntByteSize(invalidMessage, length);
                    break;
                case LONG:
                    validateLongByteSize(invalidMessage, length);
                    break;
                case FLOAT:
                    validateFloatByteSize(invalidMessage, length);
                    break;
                case DOUBLE:
                    validateDoubleByteSize(invalidMessage, length);
                    break;
                case BOOLEAN:
                    validateBooleanByteSize(invalidMessage, length);
                    break;
                case TIME:
                    validateTimeByteSize(invalidMessage, length);
                    break;
                case DATE:
                    validateDateByteSize(invalidMessage, length);
                    break;
                case TIMESTAMP:
                    validateTimeStampByteSize(invalidMessage, length);
                    break;
                case DECIMAL:
                    //decimal类型第一位是标度
                    validateDecimalByteSize(invalidMessage, length);
                    break;
                default:
                    //默认按照字符串进行处理
                    validateStringByteSize(invalidMessage, length);
                    break;
            }
        }
    }


    /**
     * 反序列化
     *
     */
    @Override
    public List< Object[] > deSerialize(Object data)
     throws StreamSerDeException
    {
        if (data == null)
        {
            LOG.debug("Input raw data is null.");
            return nullResults;
        }

        List< Object[] > splitResults = Lists.newArrayList();
        Object[] values = deserLineData((byte[])data);
        splitResults.add(values);
        return splitResults;
    }

    private Object[] deserLineData(byte[] data) throws StreamSerDeException
    {
        ByteArrayInputStream stream = new ByteArrayInputStream(data);
        Input input = new Input(stream);

        Class< ? >[] dataTypes = getSchema().getAllAttributeTypes();
        Object[] values = new Object[binaryLengthArr.length];
        try
        {
            for (int i = 0; i < binaryLengthArr.length; i++)
            {
                values[i] = readValue(input, binaryLengthArr[i], dataTypes[i]);
            }
        }
        catch (IOException e)
        {
            LOG.error("Failed to read binary data", e);
            throw new StreamSerDeException("Failed to read binary data", e);
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to read binary data", e);
            throw new StreamSerDeException("Failed to read binary data", e);
        }
        finally
        {
            StreamingUtils.close(input, stream);
        }
        return values;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object serialize(List< Object[] > events)
     throws StreamSerDeException
    {
        /*
         * 这里的输出数据类型统一按照输出schema的类型来确定
         * 因为原始数据中存在null值，无法确定属于哪种类型。
         */
        if (events == null)
        {
            LOG.info("Input event is null.");
            return null;
        }

        List< byte[] > results = Lists.newArrayList();
        Class< ? >[] dataTypes = getSchema().getAllAttributeTypes();

        try
        {

            for (Object[] event : events)
            {
                byte[] bytes = new byte[allLength];
                int offset = 0;
                for (int i = 0; i < event.length; i++)
                {
                    addBytesToEnd(bytes, getBytes(event[i], binaryLengthArr[i], dataTypes[i]), offset);
                    offset += binaryLengthArr[i];
                }
                results.add(bytes);
            }
        }
        catch (StreamingException e)
        {
            LOG.error("Failed to change value to binary data", e);
            throw new StreamSerDeException("Failed to change value to binary data", e);
        }
        return results;
    }

    private void addBytesToEnd(byte[] bytes, byte[] byteValue, int offset)
    {
        for (int i = 0; i < byteValue.length; i++)
        {
            bytes[i + offset] = byteValue[i];
        }
    }

    /**
     * 将读到的数据转为byte数组
     */
    private byte[] getBytes(Object value, int length, Class< ? > type) throws StreamingException, StreamSerDeException
    {
        byte[] results = new byte[length];
        //空值处理
        if (value == null)
        {
            return results;
        }

        byte[] bytes = toBytes(value, type);

        /**
         * 如果小于指定长度，需要补全
         */
        if (bytes.length <= length)
        {
            for (int i = 0; i < bytes.length; i++)
            {
                results[i] = bytes[i];
            }
        }
        else
        {
            //如果大于指定长度，需要输出告警，然后忽略该数据，否则就要裁剪，会导致数据错误。
            LOG.warn("data byte value is over max length, ignore it.");
            throw new StreamSerDeException("data byte value is over max length, ignore it.");
        }
        return results;
    }

    private byte[] toBytes(Object value, Class< ? > type) throws StreamingException
    {
        StreamingDataType dataType = StreamingDataType.getStreamingDataType(type);
        switch (dataType)
        {
            case INT:
                return Bytes.toBytes((Integer)value);
            case LONG:
                return Bytes.toBytes((Long)value);
            case FLOAT:
                return Bytes.toBytes((Float)value);
            case DOUBLE:
                return Bytes.toBytes((Double)value);
            case BOOLEAN:
                return Bytes.toBytes((Boolean)value);
            case TIME:
                if (isLongTimeView)
                {
                    return Bytes.toBytes(((Time)value).getTime());
                }
                else
                {
                    return Bytes.toBytes(timeParser.toStringValue(value));
                }
            case DATE:
                if (isLongTimeView)
                {
                    return Bytes.toBytes(((Date)value).getTime());
                }
                else
                {
                    return Bytes.toBytes(dateParser.toStringValue(value));
                }
            case TIMESTAMP:
                if (isLongTimeView)
                {
                    return Bytes.toBytes(((Timestamp)value).getTime());
                }
                else
                {
                    return Bytes.toBytes(timestampParser.toStringValue(value));
                }
            case DECIMAL:
                if (isDecimalNativeView)
                {
                    //decimal类型第一位是标度
                    return Bytes.toBytes((BigDecimal)value);
                }
                else
                {
                    return Bytes.toBytes(decimalParser.toStringValue(value));
                }
            default:
                //默认按照字符串进行处理
                return Bytes.toBytes((String)value);
        }
    }

    private Object readValue(Input input, Integer len, Class< ? > type)
     throws IOException, StreamingException
    {
        StreamingDataType dataType = StreamingDataType.getStreamingDataType(type);
        switch (dataType)
        {
            case INT:
                return Bytes.toInt(readBytes(input, len));
            case LONG:
                return Bytes.toLong(readBytes(input, len));
            case FLOAT:
                return Bytes.toFloat(readBytes(input, len));
            case DOUBLE:
                return Bytes.toDouble(readBytes(input, len));
            case BOOLEAN:
                return Bytes.toBoolean(readBytes(input, len));
            case TIME:
                if (isLongTimeView)
                {
                    return new Time(Bytes.toLong(readBytes(input, len)));
                }
                else
                {
                    return timeParser.createValue(removeNullBytesAtEnd(Bytes.toString(readBytes(input, len))));
                }
            case DATE:
                if (isLongTimeView)
                {
                    return new Date(Bytes.toLong(readBytes(input, len)));
                }
                else
                {
                    return dateParser.createValue(removeNullBytesAtEnd(Bytes.toString(readBytes(input, len))));
                }
            case TIMESTAMP:
                if (isLongTimeView)
                {
                    return new Timestamp(Bytes.toLong(readBytes(input, len)));
                }
                else
                {
                    return timestampParser.createValue(removeNullBytesAtEnd(Bytes.toString(readBytes(input, len))));
                }
            case DECIMAL:
                if (isDecimalNativeView)
                {
                    //decimal类型第一位是标度
                    return Bytes.toBigDecimal(readBytes(input, len));
                }
                else
                {
                    return decimalParser.createValue(removeNullBytesAtEnd(Bytes.toString(readBytes(input, len))));
                }
            default:
                //默认按照字符串进行处理
                return removeNullBytesAtEnd(Bytes.toString(readBytes(input, len)));
        }
    }

    private byte[] readBytes(Input input, int length)
     throws IOException
    {
        return input.readBytes(length);
    }

    private void formatAttributesLength(String[] lengthArr)
    {
        LOG.info("Start to init binaryLengthArr, length={}.", lengthArr.length);
        binaryLengthArr = new Integer[lengthArr.length];
        allLength = 0;
        for (int i = 0; i < lengthArr.length; i++)
        {
            binaryLengthArr[i] = Integer.valueOf(lengthArr[i]);
            allLength += binaryLengthArr[i];
        }
    }

    private void validateStringByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < 0)
        {
            String message = String.format(invalidMessage, "string", "0");
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private void validateDecimalByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < Bytes.SIZEOF_DECIMAL_MIN)
        {
            String message = String.format(invalidMessage, "decimal", String.valueOf(Bytes.SIZEOF_DECIMAL_MIN));
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private boolean validateTimeStampByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (isLongTimeView)
        {
            if (length < Bytes.SIZEOF_LONG)
            {
                String message = String.format(invalidMessage, "timestamp of long value", String.valueOf(Bytes.SIZEOF_LONG));
                LOG.error(message);
                throw new StreamSerDeException(message);
            }
            return true;
        }
        else
        {
            if (length < Bytes.SIZEOF_TIMESTAMP_STRING)
            {
                String message = String.format(invalidMessage, "timestamp of string value", String.valueOf(Bytes.SIZEOF_TIMESTAMP_STRING));
                LOG.error(message);
                throw new StreamSerDeException(message);
            }
        }
        return false;
    }

    private void validateDateByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (isLongTimeView)
        {
            if (length < Bytes.SIZEOF_LONG)
            {
                String message = String.format(invalidMessage, "date of long value", String.valueOf(Bytes.SIZEOF_LONG));
                LOG.error(message);
                throw new StreamSerDeException(message);
            }
        }
        else
        {
            if (length < Bytes.SIZEOF_DATE_STRING)
            {
                String message = String.format(invalidMessage, "date of string value", String.valueOf(Bytes.SIZEOF_DATE_STRING));
                LOG.error(message);
                throw new StreamSerDeException(message);
            }
        }
    }

    private void validateTimeByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (isLongTimeView)
        {
            if (length < Bytes.SIZEOF_LONG)
            {
                String message = String.format(invalidMessage, "time of long value", String.valueOf(Bytes.SIZEOF_LONG));
                LOG.error(message);
                throw new StreamSerDeException(message);
            }
        }
        else
        {
            if (length < Bytes.SIZEOF_TIME_STRING)
            {
                String message = String.format(invalidMessage, "time of string value", String.valueOf(Bytes.SIZEOF_TIME_STRING));
                LOG.error(message);
                throw new StreamSerDeException(message);
            }
        }
    }

    private void validateBooleanByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < Bytes.SIZEOF_BOOLEAN)
        {
            String message = String.format(invalidMessage, "boolean", String.valueOf(Bytes.SIZEOF_BOOLEAN));
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private void validateDoubleByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < Bytes.SIZEOF_DOUBLE)
        {
            String message = String.format(invalidMessage, "double", String.valueOf(Bytes.SIZEOF_DOUBLE));
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private void validateFloatByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < Bytes.SIZEOF_FLOAT)
        {
            String message = String.format(invalidMessage, "float", String.valueOf(Bytes.SIZEOF_FLOAT));
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private void validateLongByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < Bytes.SIZEOF_LONG)
        {
            String message = String.format(invalidMessage, "long", String.valueOf(Bytes.SIZEOF_LONG));
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private void validateIntByteSize(String invalidMessage, int length) throws StreamSerDeException
    {
        if (length < Bytes.SIZEOF_INT)
        {
            String message = String.format(invalidMessage, "int", String.valueOf(Bytes.SIZEOF_INT));
            LOG.error(message);
            throw new StreamSerDeException(message);
        }
    }

    private void recognizeDecimalType() throws StreamingException
    {
        String decimalType = getConfig().getStringValue(StreamingConfig.SERDE_BINARYSERDE_DECIMALYPE);
        if (STRING_VIEW.equals(decimalType.toUpperCase(Locale.US)))
        {
            isDecimalNativeView = false;
        }
        else if (DECIMAL_VIEW.equals(decimalType.toUpperCase(Locale.US)))
        {
            isDecimalNativeView = true;
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_VALUE_ERROR, decimalType);
            LOG.error(ErrorCode.CONFIG_VALUE_ERROR.getFullMessage(decimalType));
            throw exception;
        }
    }

    private void recognizeTimeType() throws StreamingException
    {
        String timeType = getConfig().getStringValue(StreamingConfig.SERDE_BINARYSERDE_TIMETYPE);
        if (STRING_VIEW.equals(timeType.toUpperCase(Locale.US)))
        {
            isLongTimeView = false;
        }
        else if (LONG_VIEW.equals(timeType.toUpperCase(Locale.US)))
        {
            isLongTimeView = true;
        }
        else
        {
            StreamingException exception = new StreamingException(ErrorCode.CONFIG_VALUE_ERROR, timeType);
            LOG.error(ErrorCode.CONFIG_VALUE_ERROR.getFullMessage(timeType));
            throw exception;
        }
    }

    private String removeNullBytesAtEnd(String str)
    {
        int lastIndex = 0;
        char[] chars = str.toCharArray();
        for (int i = chars.length - 1; i >= 0; i--)
        {
            if (chars[i] != NULL_BYTE)
            {
                //检查到最后以为不是空byte之后，取上一个字段的索引
                lastIndex = i + 1;
                break;
            }
        }

        return str.substring(0, lastIndex);
    }
}
