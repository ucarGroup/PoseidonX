package com.huawei.streaming.serde;

/**
 Copyright 2005 Bytecode Pty Ltd.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A very simple CSV parser released under a commercial-friendly license.
 * This just implements splitting a single line into fields.
 *
 */
public class CSVParser
{
    /**
     * NUM_2
     */
    public static final int NUM_2 = 2;
    
    /**
     * The default separator to use if none is supplied to the constructor.
     */
    public static final char DEFAULT_SEPARATOR = ',';
    
    /**
     * The default read size
     */
    public static final int INITIAL_READ_SIZE = 128;
    
    /**
     * The default quote character to use if none is supplied to the
     * constructor.
     */
    public static final char DEFAULT_QUOTE_CHARACTER = '"';
    
    /**
     * The default escape character to use if none is supplied to the
     * constructor.
     */
    public static final char DEFAULT_ESCAPE_CHARACTER = '\\';
    
    /**
     * The default strict quote behavior to use if none is supplied to the
     * constructor
     */
    public static final boolean DEFAULT_STRICT_QUOTES = false;
    
    /**
     * The default leading whitespace behavior to use if none is supplied to the
     * constructor
     */
    public static final boolean DEFAULT_IGNORE_LEADING_WHITESPACE = true;
    
    /**
     * This is the "null" character - if a value is set to this then it is ignored.
     * I.E. if the quote character is set to null then there is no quote character.
     */
    public static final char NULL_CHARACTER = '\0';
    
    private final char separator;
    
    private final char quotechar;
    
    private final char escape;
    
    private final boolean strictQuotes;
    
    private String pending;
    
    private boolean inField = false;
    
    private final boolean ignoreLeadingWhiteSpace;
    
    /**
     * Constructs CSVParser using a comma for the separator.
     */
    public CSVParser()
    {
        this(DEFAULT_SEPARATOR, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER);
    }
    
    /**
     * Constructs CSVParser with supplied separator.
     *
     */
    public CSVParser(char separator)
    {
        this(separator, DEFAULT_QUOTE_CHARACTER, DEFAULT_ESCAPE_CHARACTER);
    }
    
    /**
     * Constructs CSVParser with supplied separator and quote char.
     *
     */
    public CSVParser(char separator, char quotechar)
    {
        this(separator, quotechar, DEFAULT_ESCAPE_CHARACTER);
    }
    
    /**
     * Constructs CSVReader with supplied separator and quote char.
     *
     */
    public CSVParser(char separator, char quotechar, char escape)
    {
        this(separator, quotechar, escape, DEFAULT_STRICT_QUOTES);
    }
    
    /**
     * Constructs CSVReader with supplied separator and quote char.
     * Allows setting the "strict quotes" flag
     *
     */
    public CSVParser(char separator, char quotechar, char escape, boolean strictQuotes)
    {
        this(separator, quotechar, escape, strictQuotes, DEFAULT_IGNORE_LEADING_WHITESPACE);
    }
    
    /**
     * Constructs CSVReader with supplied separator and quote char.
     * Allows setting the "strict quotes" and "ignore leading whitespace" flags
     *
     */
    public CSVParser(char separator, char quotechar, char escape, boolean strictQuotes, boolean ignoreLeadingWhiteSpace)
    {
        if (anyCharactersAreTheSame(separator, quotechar, escape))
        {
            throw new UnsupportedOperationException("The separator, quote, and escape characters must be different!");
        }
        if (separator == NULL_CHARACTER)
        {
            throw new UnsupportedOperationException("The separator character must be defined!");
        }
        this.separator = separator;
        this.quotechar = quotechar;
        this.escape = escape;
        this.strictQuotes = strictQuotes;
        this.ignoreLeadingWhiteSpace = ignoreLeadingWhiteSpace;
    }
    
    private boolean anyCharactersAreTheSame(char sep, char quote, char esc)
    {
        return isSameCharacter(sep, quote) || isSameCharacter(sep, esc) || isSameCharacter(quote, esc);
    }
    
    private boolean isSameCharacter(char c1, char c2)
    {
        return c1 != NULL_CHARACTER && c1 == c2;
    }
    
    /**
     * something was left over from last call(s)
     */
    public boolean isPending()
    {
        return pending != null;
    }
    
    /**
     * Parses an incoming String and returns an array of elements.
     */
    public String[] parseLineMulti(String nextLine)
        throws IOException
    {
        return parseLine(nextLine, true);
    }
    
    /**
     * Parses an incoming String and returns an array of elements.
     */
    public String[] parseLine(String nextLine)
        throws IOException
    {
        return parseLine(nextLine, false);
    }
    
    /**
     * Parses an incoming String and returns an array of elements.
     *
     */
    private String[] parseLine(String nextLine, boolean multi)
        throws IOException
    {
        
        if (!multi && pending != null)
        {
            pending = null;
        }
        
        if (nextLine == null)
        {
            if (pending != null)
            {
                String s = pending;
                pending = null;
                return new String[] {s};
            }
            else
            {
                return null;
            }
        }
        
        List<String> tokensOnThisLine = new ArrayList<String>();
        StringBuilder sb = new StringBuilder(INITIAL_READ_SIZE);
        boolean inQuotes = false;
        if (pending != null)
        {
            sb.append(pending);
            pending = null;
            inQuotes = true;
        }
        for (int i = 0; i < nextLine.length(); i++)
        {
            
            char c = nextLine.charAt(i);
            if (c == this.escape)
            {
                if (isNextCharacterEscapable(nextLine, inQuotes || inField, i))
                {
                    sb.append(nextLine.charAt(i + 1));
                    i++;
                }
            }
            else if (c == quotechar)
            {
                if (isNextCharacterEscapedQuote(nextLine, inQuotes || inField, i))
                {
                    sb.append(nextLine.charAt(i + 1));
                    i++;
                }
                else
                {
                    //inQuotes = !inQuotes;
                    
                    // the tricky case of an embedded quote in the middle: a,bc"d"ef,g
                    if (!strictQuotes)
                    {
                        
                        //not on the beginning of the line 
                        //not at the beginning of an escape sequence 
                        //not at the  end of an escape sequence
                        if (i > NUM_2 && nextLine.charAt(i - 1) != this.separator && nextLine.length() > (i + 1)
                            && nextLine.charAt(i + 1) != this.separator)
                        {
                            
                            if (ignoreLeadingWhiteSpace && sb.length() > 0 && isAllWhiteSpace(sb))
                            {
                                sb.setLength(0); //discard white space leading up to quote
                            }
                            else
                            {
                                sb.append(c);
                                //continue;
                            }
                            
                        }
                    }
                    
                    inQuotes = !inQuotes;
                }
                inField = !inField;
            }
            else if (c == separator && !inQuotes)
            {
                tokensOnThisLine.add(sb.toString());
                sb.setLength(0); // start work on next token
                inField = false;
            }
            else
            {
                if (!strictQuotes || inQuotes)
                {
                    sb.append(c);
                    inField = true;
                }
            }
        }
        // line is done - check status
        if (inQuotes)
        {
            if (multi)
            {
                // continuing a quoted section, re-append newline
                sb.append("\n");
                pending = sb.toString();
                sb = null; // this partial content is not to be added to field list yet
            }
            else
            {
                throw new IOException("Un-terminated quoted field at end of CSV line");
            }
        }
        if (sb != null)
        {
            tokensOnThisLine.add(sb.toString());
        }
        return tokensOnThisLine.toArray(new String[tokensOnThisLine.size()]);
        
    }
    
    /**
     * precondition: the current character is a quote or an escape
     *
     */
    private boolean isNextCharacterEscapedQuote(String nextLine, boolean inQuotes, int i)
    {
        return inQuotes // we are in quotes, therefore there can be escaped quotes in here.
            && nextLine.length() > (i + 1) // there is indeed another character to check.
            && nextLine.charAt(i + 1) == quotechar;
    }
    
    /**
     * precondition: the current character is an escape
     *
     */
    protected boolean isNextCharacterEscapable(String nextLine, boolean inQuotes, int i)
    {
        return inQuotes // we are in quotes, therefore there can be escaped quotes in here.
            && nextLine.length() > (i + 1) // there is indeed another character to check.
            && (nextLine.charAt(i + 1) == quotechar || nextLine.charAt(i + 1) == this.escape);
    }
    
    /**
     * precondition: sb.length() > 0
     *
     */
    protected boolean isAllWhiteSpace(CharSequence sb)
    {
        boolean result = true;
        for (int i = 0; i < sb.length(); i++)
        {
            char c = sb.charAt(i);
            
            if (!Character.isWhitespace(c))
            {
                return false;
            }
        }
        return result;
    }
}
