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
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

/**
 * 
 * Jar文件合并
 * 
 * 将解压好的各个jar的目录文件，copy到同一个目录
 * 对于重复的文件，有多种执行策略，默认覆盖
 * 
 */
public class JarFilesMerger
{
    private static final Logger LOG = LoggerFactory.getLogger(JarFilesMerger.class);
    
    private File tmpJarUnzipDir;
    
    private File tmpOutputDir;
    
    /**
     * 默认的合并规则必需放在最后面，因为默认就是忽略重复文件
     */
    private MergeRule[] rules = {new AppendRule(), new RenameRule(), new DefaultRule()};
    
    /**
     * <默认构造函数>
     */
    public JarFilesMerger(File tmpJarUnzipDir, File tmpOutputDir)
    {
        this.tmpJarUnzipDir = tmpJarUnzipDir;
        this.tmpOutputDir = tmpOutputDir;
    }
    
    /**
     * 和并jar文件
     */
    public void mergeJarFiles()
        throws IOException
    {
        File[] jarDirs = tmpJarUnzipDir.listFiles(new DirectoryFilter());
        if (jarDirs == null)
        {
            return;
        }
        
        for (File jarsUnzipDirectory : jarDirs)
        {
            String jarName = jarsUnzipDirectory.getName();
            File[] childs = jarsUnzipDirectory.listFiles(new DirectoryFilter());
            if(childs == null)
            {
                continue;
            }

            for (File child : childs)
            {
                LOG.info("start to copy {}", child.getName());
                copyDirectory(child, new File(tmpOutputDir, child.getName()), jarName);
            }
        }
    }
    
    private void copyDirectory(File srcDir, File destDir, String jarName)
        throws IOException
    {
        List<String> exclusionList = null;
        if (destDir.getCanonicalPath().startsWith(srcDir.getCanonicalPath()))
        {
            File[] srcFiles = srcDir.listFiles();
            if (srcFiles != null && srcFiles.length > 0)
            {
                exclusionList = Lists.newArrayListWithCapacity(srcFiles.length);
                for (File srcFile : srcFiles)
                {
                    File copiedFile = new File(destDir, srcFile.getName());
                    exclusionList.add(copiedFile.getCanonicalPath());
                }
            }
        }
        doCopyDirectory(srcDir, destDir, exclusionList, jarName);
    }
    
    private void doCopyDirectory(File srcDir, File destDir, List<String> exclusionList, String jarName)
        throws IOException
    {
        File[] srcFiles = srcDir.listFiles();
        if (srcFiles == null)
        {
            throw new IOException("Src directory error, src files null.");
        }
        
        if (destDir.exists())
        {
            if (!destDir.isDirectory())
            {
                throw new IOException("Destination '" + destDir + "' exists but is not a directory");
            }
        }
        else
        {
            if (!destDir.mkdirs() && !destDir.isDirectory())
            {
                throw new IOException("Destination '" + destDir + "' directory cannot be created");
            }
        }
        
        for (File srcFile : srcFiles)
        {
            File dstFile = new File(destDir, srcFile.getName());
            if (exclusionList == null || !exclusionList.contains(srcFile.getCanonicalPath()))
            {
                if (srcFile.isDirectory())
                {
                    doCopyDirectory(srcFile, dstFile, exclusionList, jarName);
                }
                else
                {
                    doCopyFile(srcFile, dstFile, jarName);
                }
            }
        }
        
        if (!destDir.setLastModified(srcDir.lastModified()))
        {
            LOG.warn("{} setLastModified failure.", destDir.getCanonicalPath());
        }
    }
    
    private void doCopyFile(File srcFile, File destFile, String jarName)
        throws IOException
    {
        if (destFile.exists() && destFile.isDirectory())
        {
            throw new IOException("Destination '" + destFile + "' exists but is a directory");
        }
        
        if (destFile.exists())
        {
            LOG.info("{} exists.", destFile.getCanonicalPath());
            for (MergeRule rule : rules)
            {
                if (rule.match(srcFile))
                {
                    rule.execute(srcFile, destFile, jarName);
                    return;
                }
            }
        }
        Files.copy(srcFile, destFile);
    }
    
}
