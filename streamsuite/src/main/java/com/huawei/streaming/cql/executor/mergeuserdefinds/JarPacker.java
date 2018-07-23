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
import org.apache.tools.ant.taskdefs.Jar;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 创建Jar包
 * 
 */
public class JarPacker
{
    private static final Logger LOG = LoggerFactory.getLogger(JarPacker.class);
    
    /**
     * jar包内容所在目录
     */
    private File sourceDir;
    
    /**
     * 待生成的jar包文件
     */
    private String jarFile;
    
    /**
     * Jar包打包器
     */
    public JarPacker(String jarFile, File jarSourceDir)
    {
        sourceDir = jarSourceDir;
        this.jarFile = jarFile;
    }
    
    /**
     * jar包打包
     */
    public void pack()
        throws IOException
    {
        removeDistFile(jarFile);
        Jar jar = createAntJarTask(jarFile);
        jar.execute();
        LOG.info("finished to package jar");
    }
    
    private Jar createAntJarTask(String distFile)
    {
        Project prj = new Project();
        
        Jar jar = new Jar();
        jar.setProject(prj);
        jar.setDestFile(new File(distFile));
        jar.setBasedir(sourceDir);
        return jar;
    }

    private void removeDistFile(String distFile)
        throws IOException
    {
        File f = new File(distFile);
        if (f.exists())
        {
            if (f.isFile())
            {
                if (!f.delete())
                {
                    throw new IOException("Unable to delete file: " + f);
                }
            }
            else
            {
                throw new IOException("jar file is already exists and it can not be a directory!");
            }
        }
    }
    
}
