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

package com.huawei.streaming.cql.executor.mergeuserdefinds;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;

import com.google.common.collect.Sets;
import com.google.common.io.Files;

/**
 * 追加的文件规则
 * 
 */
public class AppendRule implements MergeRule
{
    private static final Charset CHARSET = Charset.forName("UTF-8");
    
    private static final String COMMENT = "# this is a comment of jar merge in cql, jar name is %s.";
    
    private Set<String> apendFiles;
    
    /**
     * <默认构造函数>
     */
    public AppendRule()
    {
        apendFiles = Sets.newHashSet();
        apendFiles.add("java.sql.Driver");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean match(File srcFile)
    {
        return apendFiles.contains(srcFile.getName());
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(File srcFile, File distFile, String jarName)
        throws IOException
    {
        //添加空行进行换行，并且防止上一个文件最后没有以换行符结尾
        Files.append(IOUtils.LINE_SEPARATOR, distFile, CHARSET);
        Files.append(IOUtils.LINE_SEPARATOR, distFile, CHARSET);
        Files.append(String.format(COMMENT, jarName), distFile, CHARSET);
        Files.append(IOUtils.LINE_SEPARATOR, distFile, CHARSET);
        String line = FileUtils.readFileToString(srcFile, CHARSET);
        Files.append(line, distFile, CHARSET);
    }
    
}
