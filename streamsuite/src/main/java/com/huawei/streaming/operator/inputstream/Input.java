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

import java.io.IOException;
import java.io.InputStream;

/** An InputStream that reads data from a byte array 
 *  and optionally fills the byte array from another OutputStream as needed.
 * Utility methods are provided for efficiently reading primitive types and strings.
 */
public class Input extends InputStream
{
    private byte[] buffer;

    private int capacity, position, limit, total;

    private InputStream inputStream;

    /** Creates a new Input for reading from a byte array.
     * @param bufferSize The size of the buffer. An exception is thrown if more bytes than this are read. */
    public Input(int bufferSize)
    {
        this.capacity = bufferSize;
        buffer = new byte[bufferSize];
    }

    /** Creates a new Input for reading from a byte array.
     * @param buffer An exception is thrown if more bytes than this are read. */
    public Input(byte[] buffer)
    {
        setBuffer(buffer, 0, buffer.length);
    }

    /** Creates a new Input for reading from a byte array.
     * @param buffer An exception is thrown if more bytes than this are read. */
    public Input(byte[] buffer, int offset, int count)
    {
        setBuffer(buffer, offset, count);
    }

    /** Creates a new Input for reading from an InputStream with a buffer size of 4096. */
    public Input(InputStream input)
    {
        this(4096);
        if (input == null)
            throw new IllegalArgumentException("inputStream cannot be null.");
        this.inputStream = input;
    }

    /** Sets a new buffer. The position and total are reset, discarding any buffered bytes. */
    public void setBuffer(byte[] bytes)
    {
        setBuffer(bytes, 0, bytes.length);
    }

    /** Sets a new buffer. The position and total are reset, discarding any buffered bytes. */
    public void setBuffer(byte[] bytes, int offset, int count)
    {
        if (bytes == null)
            throw new IllegalArgumentException("bytes cannot be null.");
        buffer = bytes;
        position = offset;
        limit = count;
        capacity = bytes.length;
        total = 0;
        inputStream = null;
    }

    /** Returns the number of bytes read. */
    public int getTotal()
    {
        return total + position;
    }

    /** Returns the current position in the buffer. */
    public int getPosition()
    {
        return position;
    }

    /** Sets the current position in the buffer. */
    public void setPosition(int position)
    {
        this.position = position;
    }

    /** Returns the limit for the buffer. */
    public int getLimit()
    {
        return limit;
    }

    /** Sets the limit in the buffer. */
    public void setLimit(int limit)
    {
        this.limit = limit;
    }

    /** Sets the position and total to zero. */
    public void rewind()
    {
        position = 0;
        total = 0;
    }

    /** Discards the specified number of bytes. */
    public void skip(int count)
        throws IOException
    {
        int skipCount = Math.min(limit - position, count);
        while (true)
        {
            position += skipCount;
            count -= skipCount;
            if (count == 0)
                break;
            skipCount = Math.min(count, capacity);
            require(skipCount);
        }
    }

    /** Fills the buffer with more bytes. Can be overridden to fill the bytes from a source other than the InputStream. */
    protected int fill(byte[] buffer, int offset, int count)
        throws IOException
    {
        if (inputStream == null)
            return -1;
        try
        {
            return inputStream.read(buffer, offset, count);
        }
        catch (IOException ex)
        {
            throw new IOException(ex);
        }
    }

    /** @param required Must be > 0. The buffer is filled until it has at least this many bytes.
     * @return the number of bytes remaining.
     * @throws IOException if EOS is reached before required bytes are read (buffer underflow). */
    private int require(int required)
        throws IOException
    {
        int remaining = limit - position;
        if (remaining >= required)
            return remaining;
        if (required > capacity)
            throw new IOException("Buffer too small: capacity: " + capacity + ", required: " + required);

        // Compact.
        System.arraycopy(buffer, position, buffer, 0, remaining);
        total += position;
        position = 0;

        while (true)
        {
            int count = fill(buffer, remaining, capacity - remaining);
            if (count == -1)
            {
                if (remaining >= required)
                    break;
                throw new IOException("Buffer underflow.");
            }
            remaining += count;
            if (remaining >= required)
                break; // Enough has been read.
        }
        limit = remaining;
        return remaining;
    }

    /** @param optional Try to fill the buffer with this many bytes.
     * @return the number of bytes remaining, but not more than optional, or -1 if the EOS was reached and the buffer is empty. */
    private int optional(int optional)
        throws IOException
    {
        int remaining = limit - position;
        if (remaining >= optional)
            return optional;
        optional = Math.min(optional, capacity);

        // Compact.
        System.arraycopy(buffer, position, buffer, 0, remaining);
        total += position;
        position = 0;

        while (true)
        {
            int count = fill(buffer, remaining, capacity - remaining);
            if (count == -1)
                break;
            remaining += count;
            if (remaining >= optional)
                break; // Enough has been read.
        }
        limit = remaining;
        return remaining == 0 ? -1 : Math.min(remaining, optional);
    }

    // InputStream

    /** Reads a single byte as an int from 0 to 255, or -1 if there are no more bytes are available. */
    public int read()
        throws IOException
    {
        if (optional(1) == 0)
            return -1;
        return buffer[position++] & 0xFF;
    }

    /** Reads bytes.length bytes or less and writes them to the specified byte[], starting at 0, and returns the number of bytes
     * read. */
    public int read(byte[] bytes)
        throws IOException
    {
        return read(bytes, 0, bytes.length);
    }

    /** Reads count bytes or less and writes them to the specified byte[], starting at offset, and returns the number of bytes read
     * or -1 if no more bytes are available. */
    public int read(byte[] bytes, int offset, int count)
        throws IOException
    {
        if (bytes == null)
            throw new IllegalArgumentException("bytes cannot be null.");
        int startingCount = count;
        int copyCount = Math.min(limit - position, count);
        while (true)
        {
            System.arraycopy(buffer, position, bytes, offset, copyCount);
            position += copyCount;
            count -= copyCount;
            if (count == 0)
                break;
            offset += copyCount;
            copyCount = optional(count);
            if (copyCount == -1)
            {
                // End of data.
                if (startingCount == count)
                    return -1;
                break;
            }
            if (position == limit)
                break;
        }
        return startingCount - count;
    }

    /** Discards the specified number of bytes. */
    public long skip(long count)
        throws IOException
    {
        long remaining = count;
        while (remaining > 0)
        {
            int skip = Math.max(Integer.MAX_VALUE, (int)remaining);
            skip(skip);
            remaining -= skip;
        }
        return count;
    }

    // byte

    /** Reads a single byte. */
    public byte readByte()
        throws IOException
    {
        require(1);
        return buffer[position++];
    }

    /** Reads a byte as an int from 0 to 255. */
    public int readByteUnsigned()
        throws IOException
    {
        require(1);
        return buffer[position++] & 0xFF;
    }

    /** Reads the specified number of bytes into a new byte[]. */
    public byte[] readBytes(int length)
        throws IOException
    {
        byte[] bytes = new byte[length];
        readBytes(bytes, 0, length);
        return bytes;
    }

    /** Reads bytes.length bytes and writes them to the specified byte[], starting at index 0. */
    public void readBytes(byte[] bytes)
        throws IOException
    {
        readBytes(bytes, 0, bytes.length);
    }

    /** Reads count bytes and writes them to the specified byte[], starting at offset. */
    public void readBytes(byte[] bytes, int offset, int count)
        throws IOException
    {
        if (bytes == null)
            throw new IllegalArgumentException("bytes cannot be null.");
        int copyCount = Math.min(limit - position, count);
        while (true)
        {
            System.arraycopy(buffer, position, bytes, offset, copyCount);
            position += copyCount;
            count -= copyCount;
            if (count == 0)
                break;
            offset += copyCount;
            copyCount = Math.min(count, capacity);
            require(copyCount);
        }
    }

}
