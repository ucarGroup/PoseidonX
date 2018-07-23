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

package com.huawei.streaming.cql;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.LineProcessor;
import com.huawei.streaming.cql.exception.CQLException;

/**
 * 纯粹CQL文件的解析
 * 
 */
public class CQLFileReader implements LineProcessor< List< String > >
{
    public static final String CHARSET_STRING = "UTF-8";

    private static final Charset CHARSET = Charset.forName(CHARSET_STRING);

    private static final Logger LOG = LoggerFactory.getLogger(CQLFileReader.class);

    private static final String CQL_LINE_SEPARATOR = ";[;\t ]*\n";

    private StringBuilder sb = null;

    private Pattern pattern = null;

    public CQLFileReader()
    {
        sb = new StringBuilder();
        pattern = Pattern.compile("\t|\r");
    }


    public void readCQLs(String file) throws CQLException
    {
        validateCQLFile(file);
        readLines(file);
    }

    /**
     * 处理文件中的每行记录，将非注释行拼接起来
     */
    @Override
    public boolean processLine(String line) throws IOException
    {
        if (isEmpty(line))
        {
            return true;
        }

        if (isCommentsLine(line.trim()))
        {
            LOG.debug("throw comments line {}", line);
            return true;
        }

        sb.append(line).append("\n");
        return true;
    }

    /**
     * 获取解析好的CQL语句
     */
    @Override
    public List< String > getResult()
    {
        List< String > results = Lists.newArrayList();
        String[] cqls = replaceBlank(sb.toString()).split(CQL_LINE_SEPARATOR);
        for (String cql : cqls)
        {
            if (isEmpty(cql))
            {
                continue;
            }

            results.add(cql);
        }

        return results;
    }

    /**
     * 是否是注释行
     */
    private boolean isCommentsLine(String newLine)
    {
        return newLine.startsWith("--") || newLine.startsWith("/*") || newLine.startsWith("*");
    }

    /**
     * 替换字符串中的空格等特殊符号
     */
    private String replaceBlank(String str)
    {
        String dest = "";
        if (str != null)
        {
            Matcher m = pattern.matcher(str);
            dest = m.replaceAll(" ");
        }
        return dest;
    }


    /**
     * 字符串是否为空
     *
     */
    private boolean isEmpty(String str)
    {
        if (Strings.isNullOrEmpty(str))
        {
            return true;
        }

        if (Strings.isNullOrEmpty(str.trim()))
        {
            return true;
        }

        return false;
    }

    /**
     * 按行读取文件内容
     */
    private void readLines(String file) throws CQLException
    {
        try
        {
            Files.readLines(new File(file), CHARSET, this);
        }
        catch (IOException e)
        {
            LOG.error("Failed to read cql file.");
            throw new CQLException("Failed to read cql file.");
        }
    }

    /**
     * 校验CQL文件，检查文件是否存在等。
     */
    private void validateCQLFile(String file) throws CQLException
    {
        File cqlFile = new File(file);

        if (!cqlFile.exists())
        {
            LOG.error("Invalid cql file, file does not exist."+cqlFile.getAbsolutePath());
            throw new CQLException("Invalid cql file, file does not exist.");
        }

        if (!cqlFile.isFile())
        {
            LOG.error("Invalid cql file, file {} does not file Type.", file);
            throw new CQLException("Invalid cql file, file " + file +" does not file Type.");
        }

    }
}
