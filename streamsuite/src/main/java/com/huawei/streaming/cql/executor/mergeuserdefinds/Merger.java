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
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.net.URLDecoder;
import java.util.UUID;

import com.google.common.base.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.api.Application;

/**
 * 合并文件
 * <p/>
 * Storm中只允许提交一个用户jar包，
 * 所以这就要求将所有的用户自定义jar包和文件打包在一个新的文件中，storm才可以加载。
 *
 */
public class Merger
{

    private static final Logger LOG = LoggerFactory.getLogger(Merger.class);

    /**
     * jar包解压缩目录的名称
     */
    private static final String JAR_UNZIP_DIR_NAME = "jartmp";

    public static final String STREAMING_WITH_DEPENDENCIES = "streaming-with-dependencies";

    /**
     * jar包存放目录
     */
    private File tmpOutputDir = null;

    /**
     * jar包解压缩目录
     */
    private File tmpJarUnzipDir = null;

    /**
     * 合并文件和Jar包到指定路径
     *
     */
    public void merge(Application app, String tmpDir, String jarOutputFile)
     throws IOException
    {
        // uuid创建jar包存放目录
        createTmpDir(tmpDir);
        // 复制jar包到jar包存放目录
        copyFilesToTmp(app);
        // 在jar包存放目录下建立tmp文件夹，解压jar包中的文件到该文件夹
        new JarExpander(tmpJarUnzipDir, tmpOutputDir).expand();
        // 将tmp目录下的各个jar包中的文件夹复制到jar包存放目录，如果重复，依据各种策略进行处理
        new JarFilesMerger(tmpJarUnzipDir, tmpOutputDir).mergeJarFiles();
        // 删除tmp目录
        FileUtils.deleteDirectory(tmpJarUnzipDir);
        // 合并jar包到指定目录
        new JarPacker(jarOutputFile, tmpOutputDir).pack();
        // 删除jar包存放目录
        FileUtils.deleteDirectory(tmpOutputDir);

    }

    private void copyFilesToTmp(Application app)
     throws IOException
    {
        String[] files = app.getUserFiles();

        if (files != null)
        {
            for (String file : files)
            {
                FileUtils.copyFileToDirectory(new File(file), tmpOutputDir);
            }
        }

        if (Strings.isNullOrEmpty(System.getProperty("cql.dependency.jar")))
        {
            //for unit test, not throw exception here.
            LOG.error("Failed to found cql.dependency.jar path in System properties.");
        }
        else
        {
            String path = System.getProperty("cql.dependency.jar");
            FileUtils.copyFileToDirectory(new File(URLDecoder.decode(path, "UTF-8")), tmpOutputDir);
        }
    }

    private void createTmpDir(String tmpDir)
     throws IOException
    {
        UUID uuid = UUID.randomUUID();
        String dirName = uuid.toString().replace("-", "");
        File baseDir = new File(tmpDir);
        tmpOutputDir = new File(baseDir, dirName);
        tmpJarUnzipDir = new File(tmpOutputDir, JAR_UNZIP_DIR_NAME);
        if (!tmpJarUnzipDir.mkdirs())
        {
            LOG.error("failed to create tmp dir");
            throw new IOException("failed to create tmp dir");
        }
    }

}
