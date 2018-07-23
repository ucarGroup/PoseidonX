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
import java.io.FileFilter;

/**
 * jar文件过滤
 * 识别一个目录下的所有的jar文件
 *
 */
public class JarFilter implements FileFilter
{
    private static final String JAR_POSTFIX_NAME = "jar";
    
    /**
     * 判断每个文件
     *
     */
    @Override
    public boolean accept(File pathname)
    {
        if (!pathname.isFile())
        {
            return false;
        }
        String postfix = pathname.getName().substring(pathname.getName().lastIndexOf(".") + 1);
        return postfix.equals(JAR_POSTFIX_NAME);
    }
    
    /**
     * 判断一个文件是否是jar包
     */
    public static boolean isJarFile(String fileName)
    {
        String postfix = fileName.substring(fileName.lastIndexOf(".") + 1);
        return postfix.equals(JAR_POSTFIX_NAME);
    }
}
