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

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.exception.StreamSerDeException;

/**
 * A very simple CSV writer released under a commercial-friendly license.
 *
 *
 */
public class CSVWriter implements Serializable
{
    private static final Logger LOG = LoggerFactory.getLogger(CSVWriter.class);

    /**
     * 注释内容
     */
    private static final long serialVersionUID = -1188706807044872265L;
    
    private static final int INITIAL_STRING_SIZE = 128;
    
    /** The character used for escaping quotes. */
    private static final char DEFAULT_ESCAPE_CHARACTER = '"';
    
    /** The default separator to use if none is supplied to the constructor. */
    private static final char DEFAULT_SEPARATOR = ',';
    
    /**
     * The default quote character to use if none is supplied to the
     * constructor.
     */
    private static final char DEFAULT_QUOTE_CHARACTER = '"';
    
    /** The quote constant to use when you wish to suppress all quoting. */
    private static final char NO_QUOTE_CHARACTER = '\u0000';
    
    /** The escape constant to use when you wish to suppress all escaping. */
    private static final char NO_ESCAPE_CHARACTER = '\u0000';

    private BaseSerDe baseSerDe;
    
    private char separator;
    
    private char quotechar;
    
    private char escapechar;

    //private ResultSetHelper resultService = new ResultSetHelperService();
    
    /**
     * Constructs CSVWriter using a comma for the separator.
     */
    public CSVWriter(BaseSerDe serde)
    {
        this(DEFAULT_SEPARATOR);
        this.baseSerDe = serde;
    }
    
    /**
     * Constructs CSVWriter with supplied separator.
     *
     *            the delimiter to use for separating entries.
     */
    private CSVWriter(char separator)
    {
        this(separator, DEFAULT_QUOTE_CHARACTER);
    }
    
    /**
     * Constructs CSVWriter with supplied separator and quote char.
     *
     *            the delimiter to use for separating entries
     *            the character to use for quoted elements
     */
    private CSVWriter(char separator, char quotechar)
    {
        this(separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
    }

    /**
     * Constructs CSVWriter with supplied separator, quote char, escape char.
     *
     *            the delimiter to use for separating entries
     *            the character to use for quoted elements
     *            the character to use for escaping quotechars or escapechars
     */
    private CSVWriter(char separator, char quotechar, char escapechar)
    {
        this.separator = separator;
        this.quotechar = quotechar;
        this.escapechar = escapechar;
    }
    
    /**
     * Writes the next line to the file.
     *
     *            a string array with each comma-separated element as a separate
     *            entry.
     */
    public String createCSV(Object[] datas) 
    {
        if (datas == null)
        {
            return null;
        }

        String[] result = null;
        try
        {
            result = baseSerDe.serializeRowToString(datas);
        }
        catch (StreamSerDeException e)
        {
            LOG.warn("One line is ignore.");
            return null;
        }

        StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int i = 0; i < datas.length; i++)
        {
            
            if (i != 0)
            {
                sb.append(separator);
            }
            
            String nextElement = result[i];

            if (nextElement == null)
            {
                continue;
            }
            
            boolean flag = stringContainsSpecialCharacters(nextElement);
            
            if (flag && quotechar != NO_QUOTE_CHARACTER)
            {
                sb.append(quotechar);
            }
            
            sb.append(flag ? processLine(nextElement) : nextElement);
            
            if (flag && quotechar != NO_QUOTE_CHARACTER)
            {
                sb.append(quotechar);
            }
        }
        return sb.toString();
    }
    
    private boolean stringContainsSpecialCharacters(String line)
    {
        return line.indexOf(quotechar) != -1 || line.indexOf(escapechar) != -1 || line.indexOf(separator) != -1;
    }
    
    private StringBuilder processLine(String nextElement)
    {
        StringBuilder sb = new StringBuilder(INITIAL_STRING_SIZE);
        for (int j = 0; j < nextElement.length(); j++)
        {
            char nextChar = nextElement.charAt(j);
            if (escapechar != NO_ESCAPE_CHARACTER && nextChar == quotechar)
            {
                sb.append(escapechar).append(nextChar);
            }
            else if (escapechar != NO_ESCAPE_CHARACTER && nextChar == escapechar)
            {
                sb.append(escapechar).append(nextChar);
            }
            else
            {
                sb.append(nextChar);
            }
        }
        
        return sb;
    }
}
