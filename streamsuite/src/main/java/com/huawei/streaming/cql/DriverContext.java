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
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.huawei.streaming.api.Application;
import com.huawei.streaming.api.UserFunction;
import com.huawei.streaming.api.streams.Schema;
import com.huawei.streaming.cql.builder.BuilderUtils;
import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.FunctionRegistry;
import com.huawei.streaming.cql.executor.mergeuserdefinds.JarFilter;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.CreateOperatorContext;
import com.huawei.streaming.cql.semanticanalyzer.parser.context.ParseContext;
import com.huawei.streaming.exception.ErrorCode;

/**
 * driver类中保存的临时数据
 * 便于初始化和清理
 *
 */
public class DriverContext
{
    private static final String FILE_PREFIX = "file://";
    
    private static final Logger LOG = LoggerFactory.getLogger(DriverContext.class);
    
    /**
     * 为每个线程生成一个命名空间，这样，流名称之类，就可以保持一致
     */
    private static ThreadLocal<BuilderUtils> builderNameSpace = new ThreadLocal<BuilderUtils>();
    
    /**
     * 客户端session内部可以使用的所有函数
     */
    private static ThreadLocal<FunctionRegistry> functions = new ThreadLocal<FunctionRegistry>();
    
    /*
     * 记录所有用户提交的CQL语句以及命令
     * 只限于对系统会做出改变的命令
     */
    private List<String> cqls = Lists.newArrayList();
    
    /*
     * 存放所有CQL的解析AST树
     * 同cqls，只限于对系统做出改变的命令
     */
    private List<ParseContext> parseContexts = Lists.newArrayList();
    
    /**
     * 流名称、schema信息
     */
    private List<Schema> schemas = Lists.newArrayList();
    
    /**
     * 用户下发的配置命令
     * 因为最终要生成XML，为了避免xml中包含很多Streaming-site中的配置属性
     * 所以这里仅仅存放用户自定义的配置属性
     */
    private TreeMap<String, String> userConfs = Maps.newTreeMap();
    
    /**
     * 用户自定义的函数
     * key：函数名称
     * value：函数信息
     */
    private TreeMap<String, UserFunction> userDefinedFunctions = Maps.newTreeMap();
    
    /**
     * 用户自定义算子名称
     * key: 算子名称
     * value: 自定义算子信息
     */
    private TreeMap<String, CreateOperatorContext> userOperators = Maps.newTreeMap();
    
    /**
     * 用户自定义的文件
     */
    private List<String> userFiles = Lists.newArrayList();
    
    /**
     * 已经编译成功或者load成功的app
     */
    private Application app;
    
    /**
     * CQL命令执行结果
     */
    private CQLResult queryResult;

    /******
     * 对应的CQL Client实例
     */
    private CQLClient cqlClient;

    /**
     * <默认构造函数>
     */
    public DriverContext(CQLClient cqlClient)
    {
        this.cqlClient = cqlClient;
        builderNameSpace.set(new BuilderUtils());
        functions.set(new FunctionRegistry());
    }
    
    public static ThreadLocal<FunctionRegistry> getFunctions()
    {
        return functions;
    }
    
    public static ThreadLocal<BuilderUtils> getBuilderNameSpace()
    {
        return builderNameSpace;
    }
    
    /**
     * 从classpath中移除jar包
     *
     */
    private static void removeFromClassPath(String[] pathsToRemove)
        throws IOException
    {
        Thread curThread = Thread.currentThread();
        URLClassLoader loader = (URLClassLoader)curThread.getContextClassLoader();
        List<URL> newPath = new ArrayList<URL>(Arrays.asList(loader.getURLs()));
        
        if (pathsToRemove != null)
        {
            for (String onestr : pathsToRemove)
            {
                if (StringUtils.indexOf(onestr, FILE_PREFIX) == 0)
                {
                    onestr = StringUtils.substring(onestr, CQLConst.I_7);
                }
                
                URL oneurl = (new File(onestr)).toURI().toURL();
                
                Iterator<URL> iterator = newPath.iterator();
                while (iterator.hasNext())
                {
                    URL url = iterator.next();
                    if (url.equals(oneurl))
                    {
                        iterator.remove();
                    }
                }
            }
        }

        loader = new URLClassLoader(newPath.toArray(new URL[newPath.size()]));
        curThread.setContextClassLoader(loader);
    }
    
    public List<String> getUserFiles()
    {
        return userFiles;
    }
    
    public TreeMap<String, UserFunction> getUserDefinedFunctions()
    {
        return userDefinedFunctions;
    }
    
    /**
     * 添加schema
     *
     */
    public void addSchema(Schema schema)
    {
        schemas.add(schema);
    }
    
    /**
     * 用户下发自定义参数
     *
     */
    public void addConf(String key, String value)
    {
        if (value == null)
        {
            userConfs.remove(key);
        }
        else
        {
            userConfs.put(key, value);
        }
    }
    
    /**
     * 添加用户自定义函数
     *
     */
    public void addUserDefoundFunctions(UserFunction userFunction)
    {
        userDefinedFunctions.put(userFunction.getName(), userFunction);
    }
    
    /**
     * 移除用户自定义函数
     *
     */
    public void removeUserDefinedFunction(String functionName, boolean checkExists)
    {
        if (checkExists)
        {
            if (userDefinedFunctions.containsKey(functionName))
            {
                userDefinedFunctions.remove(functionName);
            }
            
        }
        else
        {
            userDefinedFunctions.remove(functionName);
        }
    }
    
    /**
     * 添加文件
     *
     */
    public void addFile(String file)
    {
        this.userFiles.add(file);
    }
    
    /**
     * 添加jar包，并将其注册到系统内部，使得其他自定义接口可以使用
     *
     */
    public void addJar(String jar)
        throws ExecutorException
    {
        if (jar == null)
        {
            LOG.info("No jar add to class path.");
            return;
        }
        
        registerJar(jar);
        this.userFiles.add(jar);
    }
    
    /**
     * 清空应用程序
     *
     */
    public void cleanApp()
    {
        app = null;
    }
    
    /**
     * 添加命令
     *
     */
    public void addCQLs(String cql)
    {
        cqls.add(cql);
        
    }
    
    /**
     * 添加解析结果
     *
     */
    public void addParseContext(ParseContext parseContext)
    {
        parseContexts.add(parseContext);
    }
    
    /**
     * 清除临时对象
     *
     */
    public void clean()
    {
        unRegisterJars();
        this.app = null;
        this.parseContexts.clear();
        this.cqls.clear();
        this.queryResult = null;
        this.schemas.clear();
        this.userConfs.clear();
        this.userFiles.clear();
        this.userDefinedFunctions.clear();
        this.userOperators.clear();
        builderNameSpace.remove();
        builderNameSpace.set(new BuilderUtils());
        functions.remove();
        functions.set(new FunctionRegistry());
    }
    
    public List<String> getCqls()
    {
        return cqls;
    }
    
    public Application getApp()
    {
        return app;
    }
    
    public void setApp(Application app)
    {
        this.app = app;
    }
    
    public CQLResult getQueryResult()
    {
        return queryResult;
    }
    
    public void setQueryResult(CQLResult queryResult)
    {
        this.queryResult = queryResult;
    }
    
    public List<Schema> getSchemas()
    {
        return schemas;
    }
    
    public TreeMap<String, String> getUserConfs()
    {
        return userConfs;
    }
    
    public List<ParseContext> getParseContexts()
    {
        return parseContexts;
    }
    
    private void registerJar(String jar)
        throws ExecutorException
    {
        
        ClassLoader loader = Thread.currentThread().getContextClassLoader();
        
        ClassLoader newLoader = addToClassPath(loader, StringUtils.split(jar, ","));
        Thread.currentThread().setContextClassLoader(newLoader);
        LOG.info("Added " + jar + " to class path");
        
    }
    
    private ClassLoader addToClassPath(ClassLoader classLoader, String[] newPaths)
        throws ExecutorException
    {
        URLClassLoader loader = (URLClassLoader)classLoader;
        List<URL> curPath = Arrays.asList(loader.getURLs());
        List<URL> newPath = Lists.newArrayList();
        
        for (URL onePath : curPath)
        {
            newPath.add(onePath);
        }
        curPath = newPath;
        if (newPaths != null)
        {
            for (String onestr : newPaths)
            {
                if (StringUtils.indexOf(onestr, FILE_PREFIX) == 0)
                {
                    onestr = StringUtils.substring(onestr, CQLConst.I_7);
                }
                
                URL oneurl = getFileURL(onestr);
                
                if (!curPath.contains(oneurl))
                {
                    curPath.add(oneurl);
                }
            }
        }
        
        return new URLClassLoader(curPath.toArray(new URL[curPath.size()]), loader);
    }
    
    private URL getFileURL(String onestr)
        throws ExecutorException
    {
        try
        {
            return (new File(onestr)).toURI().toURL();
        }
        catch (MalformedURLException e)
        {
            ExecutorException exception = 
                new ExecutorException(ErrorCode.SEMANTICANALYZE_REGISTER_JAR, new File(onestr).getName());
            LOG.error("Failed to parse file path.", exception);
            throw exception;
        }
    }
    
    private void unRegisterJars()
    {
        LOG.info("unRegister jars from class loader.");
        for (String jar : this.userFiles)
        {
            if (jar == null)
            {
                LOG.info("No file to delete from class path.");
                continue;
            }
            
            if (JarFilter.isJarFile(jar))
            {
                unRegisterJar(jar);
            }
        }
    }
    
    private void unRegisterJar(String jar)
    {
        try
        {
            removeFromClassPath(StringUtils.split(jar, ","));
            LOG.info("Deleted " + jar + " from class path");
        }
        catch (IOException e)
        {
            LOG.error("Unable to unRegister jar {} for io error.", jar);
        }
    }

    /**
     * 添加用户自定义算子定义
     */
    public void addUserOperator(CreateOperatorContext createOp)
    {
        userOperators.put(createOp.getOperatorName(), createOp);
    }

    
    public TreeMap<String, CreateOperatorContext> getUserOperators()
    {
        return userOperators;
    }

    public void setUserOperators(TreeMap<String, CreateOperatorContext> userOperators)
    {
        this.userOperators = userOperators;
    }

    public CQLClient getCqlClient() {
        return cqlClient;
    }
}
