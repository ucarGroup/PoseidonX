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

package com.huawei.streaming.cql.tasks;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.List;

import org.apache.tools.zip.ZipFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.CQLResult;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.cql.hooks.SemanticAnalyzeHook;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.AddJarStatementContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * add jar 命令执行器
 *
 */
public class AddJarTask extends BasicTask
{
    private static final Logger LOG = LoggerFactory.getLogger(AddJarTask.class);
    
    private static final String JAR_POSTFIX_NAME = "jar";
    
    private static final String ENCODING = "UTF8";
    
    private DriverContext context;
    
    private AddJarStatementContext addJarContext;
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void init(DriverContext driverContext, StreamingConfig config, List<SemanticAnalyzeHook> analyzeHooks)
        throws CQLException
    {
        super.init(driverContext, config, analyzeHooks);
        this.context = driverContext;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void execute(ParseContext parseContext)
        throws CQLException
    {
        addJarContext = (AddJarStatementContext)parseContext;
        String filePath = addJarContext.getPath();
        File file = new File(filePath);
        validateFile(filePath, file);
        context.addJar(filePath);
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CQLResult getResult()
    {
        return null;
    }
    
    private void checkIsJar(String filePath, File file)
        throws CQLException
    {
        String postfix = file.getName().substring(file.getName().lastIndexOf(".") + 1);
        if (!postfix.equals(JAR_POSTFIX_NAME))
        {
            CQLException exception = new CQLException(ErrorCode.SEMANTICANALYZE_FILE_NOT_JAR, filePath);
            LOG.error("File not a jar type.", exception);
            throw exception;
        }
    }
    
    private void validateFile(String filePath, File file)
        throws CQLException
    {
        checkFileExists(filePath, file);
        checkIsFile(filePath, file);
        checkIsJar(filePath, file);
        checkJarContext(filePath, file);
    }
    
    private void checkJarContext(String filePath, File file)
        throws CQLException
    {
        ZipFile zipFile = null;
        try
        {
            /*
             * 检查文件类型是否是zip文件，ANT中会自动进行检查，无需解压缩
             */
            zipFile = new ZipFile(file, ENCODING, true);
        }
        catch (FileNotFoundException e)
        {
            CQLException exception = new CQLException(ErrorCode.SEMANTICANALYZE_FILE_NOT_EXISTS, filePath);
            LOG.error("File does not exists.", exception);
            throw exception;
        }
        catch (IOException e)
        {
            CQLException exception = new CQLException(ErrorCode.SEMANTICANALYZE_FILE_NOT_JAR, filePath);
            LOG.error("File not a jar type.", exception);
            throw exception;
        }
        finally
        {
            ZipFile.closeQuietly(zipFile);
        }
    }
    
    private void checkIsFile(String filePath, File file)
        throws CQLException
    {
        if (!file.isFile())
        {
            CQLException exception = new CQLException(ErrorCode.SEMANTICANALYZE_FILE_IS_DIR, filePath);
            LOG.error("Parameter in path not a file.", exception);
            throw exception;
        }
    }
    
    private void checkFileExists(String filePath, File file)
        throws CQLException
    {
        if (!file.exists())
        {
            CQLException exception = new CQLException(ErrorCode.SEMANTICANALYZE_FILE_NOT_EXISTS, filePath);
            LOG.error("File in path does not exists.", exception);
            throw exception;
        }
    }
}
