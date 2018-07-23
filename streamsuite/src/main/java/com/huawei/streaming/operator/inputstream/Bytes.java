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
package com.huawei.streaming.operator.inputstream;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;

import com.huawei.streaming.util.datatype.TimeConstants;

/**
 * Utility class that handles byte arrays, conversions to/from other types,
 * comparisons, hash code generation, manufacturing keys for HashMaps or
 * HashSets, etc.
 */
public class Bytes
{
    public static final Charset CHARSET = Charset.forName("UTF-8");

    /**
     * Size of boolean in bytes
     */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /**
     * Size of double in bytes
     */
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    /**
     * Size of float in bytes
     */
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    /**
     * Size of int in bytes
     */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /**
     * Size of long in bytes
     */
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * decimal类型的最小byte长度
     */
    public static final int SIZEOF_DECIMAL_MIN = 5;

    /**
     * 字符串格式时间类型长度
     */
    public static final int SIZEOF_TIME_STRING = TimeConstants.TIME_FORMAT.getBytes(CHARSET).length;

    /**
     * 日期格式字符串长度
     */
    public static final int SIZEOF_DATE_STRING = TimeConstants.DATE_FORMAT.getBytes(CHARSET).length;

    /**
     * timestamp类型字符串长度
     */
    public static final int SIZEOF_TIMESTAMP_STRING = TimeConstants.TIMESTAMP_FORMAT.getBytes(CHARSET).length;

    /**
     * Put bytes at the specified byte array position.
     *
     */
    private static int putBytes(byte[] tgtBytes, int tgtOffset, byte[] srcBytes, int srcOffset, int srcLength)
    {
        System.arraycopy(srcBytes, srcOffset, tgtBytes, tgtOffset, srcLength);
        return tgtOffset + srcLength;
    }


    /**
     * toString
     *
     */
    public static String toString(final byte[] b)
    {
        if (b == null)
        {
            return null;
        }
        return toString(b, 0, b.length);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * an UnsupportedEncodingException occurs, this method will eat it
     * and return null instead.
     *
     */
    private static String toString(final byte[] b, int off, int len)
    {
        if (b == null)
        {
            return null;
        }
        if (len == 0)
        {
            return "";
        }
        try
        {
            return new String(b, off, len, "UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            return null;
        }
    }

    /**
     * Converts a string to a UTF-8 byte array.
     *
     */
    public static byte[] toBytes(String s)
    {
        try
        {
            return s.getBytes("UTF-8");
        }
        catch (UnsupportedEncodingException e)
        {
            return null;
        }
    }

    /**
     * Convert a boolean to a byte array. True becomes -1
     * and false becomes 0.
     *
     */
    public static byte[] toBytes(final boolean b)
    {
        return new byte[] {b ? (byte)-1 : (byte)0};
    }

    /**
     * Reverses {@link #toBytes(boolean)}
     *
     */
    public static boolean toBoolean(final byte[] b)
    {
        if (b.length != 1)
        {
            throw new IllegalArgumentException("Array has wrong size: " + b.length);
        }
        return b[0] != (byte)0;
    }

    /**
     * Convert a long value to a byte array using big-endian.
     *
     */
    public static byte[] toBytes(long val)
    {
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--)
        {
            b[i] = (byte)val;
            val >>>= 8;
        }
        b[0] = (byte)val;
        return b;
    }

    /**
     * Converts a byte array to a long value. Reverses
     * {@link #toBytes(long)}
     *
     */
    public static long toLong(byte[] bytes)
    {
        return toLong(bytes, 0, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value.
     *
     * if there's not enough room in the array at the offset indicated.
     */
    private static long toLong(byte[] bytes, int offset, final int length)
    {
        if (offset + length > bytes.length)
        {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
        }
        long l = 0;
        for (int i = offset; i < offset + length; i++)
        {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    private static IllegalArgumentException explainWrongLengthOrOffset(final byte[] bytes, final int offset,
     final int length, final int expectedLength)
    {
        String reason;
        if (length != expectedLength)
        {
            reason = "Wrong length: " + length + ", expected " + expectedLength;
        }
        else
        {
            reason =
             "offset (" + offset + ") + length (" + length + ") exceed the" + " capacity of the array: "
              + bytes.length;
        }
        return new IllegalArgumentException(reason);
    }


    /**
     * Presumes float encoded as IEEE 754 floating-point "single format"
     *
     */
    public static float toFloat(byte[] bytes)
    {
        return toFloat(bytes, 0);
    }

    /**
     * Presumes float encoded as IEEE 754 floating-point "single format"
     *
     */
    private static float toFloat(byte[] bytes, int offset)
    {
        return Float.intBitsToFloat(toInt(bytes, offset, SIZEOF_INT));
    }


    /**
     */
    public static byte[] toBytes(final float f)
    {
        // Encode it as int
        return Bytes.toBytes(Float.floatToRawIntBits(f));
    }

    /**
     */
    public static double toDouble(final byte[] bytes)
    {
        return toDouble(bytes, 0);
    }

    /**
     */
    private static double toDouble(final byte[] bytes, final int offset)
    {
        return Double.longBitsToDouble(toLong(bytes, offset, SIZEOF_LONG));
    }


    /**
     * Serialize a double as the IEEE 754 double format output. The resultant
     * array will be 8 bytes long.
     *
     */
    public static byte[] toBytes(final double d)
    {
        // Encode it as a long
        return Bytes.toBytes(Double.doubleToRawLongBits(d));
    }

    /**
     * Convert an int value to a byte array
     *
     */
    public static byte[] toBytes(int val)
    {
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--)
        {
            b[i] = (byte)val;
            val >>>= 8;
        }
        b[0] = (byte)val;
        return b;
    }

    /**
     * Converts a byte array to an int value
     *
     */
    public static int toInt(byte[] bytes)
    {
        return toInt(bytes, 0, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     *
     */
    private static int toInt(byte[] bytes, int offset)
    {
        return toInt(bytes, offset, SIZEOF_INT);
    }

    /**
     * Converts a byte array to an int value
     *
     * if there's not enough room in the array at the offset indicated.
     */
    private static int toInt(byte[] bytes, int offset, final int length)
    {
        if (offset + length > bytes.length)
        {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_INT);
        }

        int n = 0;
        for (int i = offset; i < (offset + length); i++)
        {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    /**
     * Put an int value out to the specified byte array position.
     *
     * enough room at the offset specified.
     */
    private static int putInt(byte[] bytes, int offset, int val)
    {
        if (bytes.length - offset < SIZEOF_INT)
        {
            throw new IllegalArgumentException("Not enough room to put an int at" + " offset " + offset + " in a "
             + bytes.length + " byte array");
        }
        for (int i = offset + 3; i > offset; i--)
        {
            bytes[i] = (byte)val;
            val >>>= 8;
        }
        bytes[offset] = (byte)val;
        return offset + SIZEOF_INT;
    }


    /**
     * Convert a BigDecimal value to a byte array
     *
     */
    public static byte[] toBytes(BigDecimal val)
    {
        byte[] valueBytes = val.unscaledValue().toByteArray();
        byte[] result = new byte[valueBytes.length + SIZEOF_INT];
        int offset = putInt(result, 0, val.scale());
        putBytes(result, offset, valueBytes, 0, valueBytes.length);
        return result;
    }

    /**
     * Converts a byte array to a BigDecimal
     *
     */
    public static BigDecimal toBigDecimal(byte[] bytes)
    {
        return toBigDecimal(bytes, 0, bytes.length);
    }

    /**
     * Converts a byte array to a BigDecimal value
     *
     */
    private static BigDecimal toBigDecimal(byte[] bytes, int offset, final int length)
    {
        if (bytes == null || length < SIZEOF_INT + 1 || (offset + length > bytes.length))
        {
            return null;
        }

        int scale = toInt(bytes, 0);
        byte[] tcBytes = new byte[length - SIZEOF_INT];
        System.arraycopy(bytes, SIZEOF_INT, tcBytes, 0, length - SIZEOF_INT);
        return new BigDecimal(new BigInteger(tcBytes), scale);
    }
}
