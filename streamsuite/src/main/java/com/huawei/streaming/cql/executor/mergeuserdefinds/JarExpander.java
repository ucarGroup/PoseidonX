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

import org.apache.tools.ant.Project;
import org.apache.tools.ant.taskdefs.Expand;
import org.apache.tools.ant.types.FileSet;
import org.apache.tools.ant.types.PatternSet;
import org.apache.tools.ant.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jar文件展开
 * 将一个jar文件展开到指定目录
 *
 */
public class JarExpander
{
    private static final Logger LOG = LoggerFactory.getLogger(JarExpander.class);
    
    /**
     * 解压缩的目录，解压缩的jar包会放在该目录
     */
    private File expandDir;
    
    /**
     * jar包所在目录，下面可能存在多个jar包，需要一一解压缩
     */
    private File jarsDir;
    
    /**
     * 解压缩jar包到指定目录
     *
     */
    public JarExpander(File expandDir, File jarsDir)
    {
        this.expandDir = expandDir;
        this.jarsDir = jarsDir;
    }
    
    /**
     * 解压缩jar包
     *
     */
    public void expand()
        throws IOException
    {
        File[] fs = jarsDir.listFiles(new JarFilter());
        if (fs == null)
        {
           return; 
        }
        
        for (File f : fs)
        {
            LOG.info("start to unzip jar {}", f.getName());
            unzipJar(f.getCanonicalPath());
            FileUtils.delete(f);
        }

        LOG.info("finished to unzip jar to dir");
    }
    
    private void unzipJar(String jar)
        throws IOException
    {
        File jarFile = new File(jar);
        LOG.info("unzip jar {} to {}", jar, expandDir.getCanonicalPath());
        Expand expand = createExpand(jarFile);
        expand.execute();
    }
    
    private Expand createExpand(File jarFile)
    {
        String outputDir = expandDir + File.separator + jarFile.getName();

        Project prj = new Project();
        FileSet fileSet = createFileSetForJarFile(jarFile, prj);
        PatternSet patternSet = createPatternSet(prj);
        Expand expand = new Expand();
        expand.setProject(prj);
        expand.setOverwrite(true);
        expand.setDest(new File(outputDir));
        expand.addFileset(fileSet);
        expand.addPatternset(patternSet);
        return expand;
    }
    
    private PatternSet createPatternSet(Project prj)
    {
        PatternSet patternSet = new PatternSet();
        patternSet.setProject(prj);
        patternSet.setIncludes("**/*");
        return patternSet;
    }
    
    private FileSet createFileSetForJarFile(File jarFile, Project prj)
    {
        FileSet fileSet = new FileSet();
        fileSet.setProject(prj);
        fileSet.setFile(jarFile);
        return fileSet;
    }
}
