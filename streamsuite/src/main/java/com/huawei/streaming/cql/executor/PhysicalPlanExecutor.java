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

package com.huawei.streaming.cql.executor;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.commons.io.FileUtils;
import org.apache.tools.ant.BuildException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.PhysicalPlan;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.executorplanvalidater.ExecutorPlanChecker;
import com.huawei.streaming.cql.executor.mergeuserdefinds.JarFilter;
import com.huawei.streaming.cql.executor.mergeuserdefinds.Merger;
import com.huawei.streaming.cql.executor.pyhsicplanvalidater.PhysicPlanChecker;
import com.huawei.streaming.cql.hooks.ExecutorHook;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.huawei.streaming.exception.StreamingRuntimeException;

/**
 * 物理计划执行器
 *
 */
public class PhysicalPlanExecutor implements ExecutorHook
{
    
    private static final Logger LOG = LoggerFactory.getLogger(PhysicalPlanExecutor.class);

    /**
     * 1KB文件大小
     */
    private static final long ONE_KB = 1024;

    /**
     * 1MB文件的大小
     */
    private static final long ONE_MB = ONE_KB * ONE_KB;

    private ExecutorPlanGenerator generator = null;
    
    private ExecutorPlanChecker executorChecker = null;
    
    private List<ExecutorHook> executorHooks = null;
    
    /*
     * 为了防止通过其他非Driver入口提交任务的时候，
     * DriverContext中的ThreadLocal数据没有初始化的问题，
     * 在executor入口这里再次继续初始化
     */
    private DriverContext driverContext = null;
    
    private String userPackagedJar = null;

    private StreamingConfig config = null;

    /**
     * <默认构造函数>
     */
    public PhysicalPlanExecutor()
    {
        executorChecker = new ExecutorPlanChecker();
        generator = new ExecutorPlanGenerator();
        executorHooks = Lists.newArrayList();
        config = new StreamingConfig();
    }

    /**
     * 执行计划
     * <p/>
     * 1、加载执行计划
     * 2、组装执行计划
     * 3、用户自定义处理
     * 4、表达式解析
     * 5、application构建
     * 6、执行计划检查
     * 数据类型检查
     * 环路检查
     * 表达式检查
     * 7、提交执行计划
     *
     */
    public void execute(String path,DriverContext driverContextParam)
        throws ExecutorException   
    {
        PhysicalPlan plan = PhysicalPlanLoader.load(path);
        execute(plan.getApploication(),driverContextParam);
    }
    
    /**
     * 为后期cql直接提交预留接口
     *
     */
    public void execute(Application apiApplication,DriverContext driverContextParam)
        throws ExecutorException
    {
        LOG.info("start to execute application {}", apiApplication.getApplicationId());
        boolean isStartFromDriver = true;

        if (DriverContext.getFunctions().get() == null)
        {
            this.driverContext = driverContextParam;
            isStartFromDriver = false;
        }

        try
        {
            parseUserDefineds(apiApplication, isStartFromDriver);
            com.huawei.streaming.application.Application app = generatorPlan(apiApplication,driverContextParam);
            submit(app);
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
        catch (StreamingRuntimeException e)
        {
            //防止表达式创建或者算子实例创建的时候抛出runtime异常
            //不过这里只捕获StreamingRuntimeException异常
            //RuntimeException计划后期全部替换成StreamingRuntimeException
            throw ExecutorException.wrapStreamingRunTimeException(e);
        }
        finally
        {
            if (userPackagedJar != null)
            {
                LOG.info("delete user packed jar after submit,path:"+userPackagedJar);
                //FileUtils.deleteQuietly(new File(userPackagedJar));
            }

            if (driverContext != null)
            {
                driverContext.clean();
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void preExecute(Application physicPlanApplication)
    {
        for (int i = 0; i < executorHooks.size(); i++)
        {
            executorHooks.get(i).preExecute(physicPlanApplication);
        }
        
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void preSubmit(com.huawei.streaming.application.Application submitApplication)
    {
        for (int i = 0; i < executorHooks.size(); i++)
        {
            executorHooks.get(i).preSubmit(submitApplication);
        }
    }
    
    /**
     * 提交应用程序
     *
     */
    private void submit(com.huawei.streaming.application.Application app)
        throws ExecutorException
    {
        LOG.info("start to submit application {}", app.getAppName());
        if (userPackagedJar != null)
        {
            app.setUserPackagedJar(userPackagedJar);
            
        }
        try
        {
            app.launch();
        }
        catch (StreamingException e)
        {
            throw ExecutorException.wrapStreamingException(e);
        }
    }
    
    /**
     * 生成执行计划
     *
     */
    private com.huawei.streaming.application.Application generatorPlan(Application apiApplication, DriverContext driverContext)
        throws ExecutorException
    {
        //执行器执行之前的钩子
        preExecute(apiApplication);
        
        new PhysicPlanChecker().check(apiApplication);
        
        /*
         * 用户自定义的处理
         * 执行计划的组装
         * 构建application
         * 表达式的解析被延迟到这里来实现
         */
        com.huawei.streaming.application.Application app = generator.generate(apiApplication,driverContext);
        
        //提交执行计划之前的钩子
        preSubmit(app);
        
        /*
         * 执行计划检查
         */
        executorChecker.check(app);
        return app;
    }
    
    private void parseUserDefineds(Application apiApplication, boolean isStartupFromDriver)
        throws StreamingException
    {
        String[] userFiles = apiApplication.getUserFiles();
        if (!isStartupFromDriver)
        {
            //注册jar包
            addJars(userFiles);
            //注册函数
            registerFunctions(apiApplication.getUserFunctions());
        }
        
        //打包jar包
        packageJar(apiApplication);
        //检查jar包大小
        checkUserJarSize();
    }

    private void checkUserJarSize() throws StreamingException
    {
        try
        {
            File file = new File(userPackagedJar).getCanonicalFile();
            String tmpDir = new File(config.getStringValue(StreamingConfig.STREAMING_TEMPLATE_DIRECTORY)).getCanonicalPath();
           
            if (!file.getPath().startsWith(tmpDir))
            {
                LOG.error("Invalid user jar path, not in config template path.");
                throw new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            }
            
            if(!file.exists())
            {
                LOG.error("Can't found submitted jar.");
                ExecutorException exception = new ExecutorException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
                throw exception;
            }

            if(!file.isFile())
            {
                LOG.error("Submitted jar is not a jar file.");
                ExecutorException exception = new ExecutorException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
                throw exception;
            }

            if(!JarFilter.isJarFile(userPackagedJar))
            {
                LOG.error("Submitted jar is not a jar file.");
                ExecutorException exception = new ExecutorException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
                throw exception;
            }

            int maxFileSize = config.getIntValue(StreamingConfig.STREAMING_USERFILE_MAXSIZE);
            long length = file.length();

            if(length > (maxFileSize * ONE_MB))
            {
                ExecutorException exception = new ExecutorException(ErrorCode.SEMANTICANALYZE_USERFILE_OVER_MAXSIZE,
                    String.valueOf(maxFileSize));
                LOG.error("Submitted jar size than max size.");
                throw exception;
            }
        }
        catch (IOException e)
        {
            LOG.error("Failed to get canonical pathname for io error.");
            ExecutorException exception = new ExecutorException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
        catch (SecurityException e1)
        {
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to get canonical pathname for cannot be accessed.", exception);
            throw exception;
        }
    }

    /**
     * 生成Jar包
     * 不论是否包含用户文件
     * 如果包含，就生成一个完整的大jar包
     * 如果不包含，就生成一个空的Jar包
     */
    private void packageJar(Application apiApplication)
     throws StreamingException
    {
        
        //Jar包的输出路径
        String prefix = apiApplication.getApplicationId() + ".";
        String postFix = ".jar";
        UUID uuid = UUID.randomUUID();
        String fileNameBody = uuid.toString().replace("-", "");
        
        String tmpDir = config.getStringValue(StreamingConfig.STREAMING_TEMPLATE_DIRECTORY);
        try
        {
            tmpDir = new File(tmpDir).getCanonicalPath();
        }
        catch (IOException e)
        {
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to get canonical pathname for io error.", exception);
            throw exception;
        }
        catch (SecurityException e1)
        {
            StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to get canonical pathname for cannot be accessed.", exception);
            throw exception;
        }
        
        userPackagedJar = tmpDir + File.separator + prefix + fileNameBody + postFix;
        Merger merger = new Merger();
        try
        {
            merger.merge(apiApplication, tmpDir, userPackagedJar);
        }
        catch (IOException e)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            LOG.error("Failed to merge all user files to one jar for io error.", exception); 
            throw exception;
        }      
        catch (BuildException e)
        {
            LOG.error("Failed to merge all user files to one jar", e);
            ExecutorException exception = new ExecutorException(e, ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
            throw exception;
        }
    }
    
    private void registerFunctions(List<UserFunction> functions)
    {
        if (functions == null)
        {
            return;
        }
        
        for (UserFunction function : functions)
        {
            driverContext.addUserDefoundFunctions(function);
        }
    }
    
    private void addJars(String[] userFiles)
        throws ExecutorException
    {
        if (userFiles == null)
        {
            return;
        }
        
        for (String jar : userFiles)
        {
            if (JarFilter.isJarFile(jar))
            {
                driverContext.addJar(jar);
            }
        }
    }
    
}
