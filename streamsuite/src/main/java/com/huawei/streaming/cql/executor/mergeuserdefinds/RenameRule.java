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
import java.util.Locale;
import java.util.Set;

import org.apache.commons.io.FileUtils;

import com.google.common.collect.Sets;

/**
 * 重命名的规则
 * 
 */
public class RenameRule implements MergeRule
{
    
    private Set<String> renameFiles;
    
    /**
     * <默认构造函数>
     */
    public RenameRule()
    {
        renameFiles = Sets.newHashSet();
        renameFiles.add("LICENSE");
        renameFiles.add("NOTICE");
        renameFiles.add("DEPENDENCIES");
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean match(File srcFile)
    {
        String fname = srcFile.getName().toUpperCase(Locale.US);
        for (String str : renameFiles)
        {
            if (fname.contains(str))
            {
                return true;
            }
        }
        return false;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(File srcFile, File distFile, String jarName)
        throws IOException
    {
        String parent = distFile.getParent();
        String newFilePath = parent + File.separator + jarName + "." + srcFile.getName();
        FileUtils.copyFile(srcFile, new File(newFilePath));
    }
    
}
