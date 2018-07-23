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
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import com.ucar.streamsuite.common.util.ExceptionUtils;
import jline.console.ConsoleReader;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Hierarchy;
import org.apache.log4j.spi.DefaultRepositorySelector;
import org.apache.log4j.spi.LoggerRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.huawei.streaming.cql.exception.CQLException;
import com.huawei.streaming.exception.ErrorCode;

/**
 * CQL的client，客户端程序入口
 * 这里会启动一个命令行的客户端
 * <p/>
 * 支持多个参数，既可以直接读取文件，也可以接受控制台输入。
 *
 */
public class CQLClient
{
    
    private static final Logger LOG = LoggerFactory.getLogger(CQLClient.class);
    
    private static final String DEFAULT_CLI_TIP = "Streaming>";
    
    private static final String WAITING_INPUT_TIP = ">";
    
    private static final String PRINTSTREAM_CHARSET = "UTF-8";
    
    private static final String CQL_EXIT_CMD = "exit";
    
    private static final String SUBMIT_COMMAND = "SUBMIT";
    
    private static final String TEMP_FILE_FOR_SUSE = ".inputrc";
    
    private static final int ERROR_EXIT_CODE = 1;
    
    private CQLSessionState ss;
    
    private Driver driver;

    /***
     * 自定义配置参数信息
     */
    private Map<String,Object> customizedConfigurationMap = new HashMap<String, Object>();

    /**
     * 命令行客户端入口
     *
     */
    public static void main(String[] args) throws IOException
    {
        disableLog4j();
        int result = CQLSessionState.STATE_OK;
        CQLClient client = new CQLClient();
        
        result = client.initSessionState();
        if (result != CQLSessionState.STATE_OK)
        {
            System.exit(result);
        }
        
        if (!client.parseArgs(args))
        {
            System.exit(CQLSessionState.STATE_NORMAL_EXIT);
        }
        
        client.createTempFile();
        
        result = client.executeDriver();
        System.exit(result);
    }
    
    /**
     * 初始化用户session
     *
     */
    public int initSessionState()
    {
        LOG.debug("start to initlize session state");
        ss = new CQLSessionState();
        try
        {
            ss.setIn(System.in);
            ss.setOut(new PrintStream(System.out, true, PRINTSTREAM_CHARSET));
            ss.setInfo(new PrintStream(System.err, true, PRINTSTREAM_CHARSET));
            ss.setErr(new PrintStream(System.err, true, PRINTSTREAM_CHARSET));
        }
        catch (UnsupportedEncodingException e)
        {
            return CQLConst.I_3;
        }
        return 0;
    }


    /**
     * 解析系统参数
     *
     */
    public boolean parseArgs(String[] args)
    {
        LOG.debug("start to parse cli args");
        OptionsProcessor processor = new OptionsProcessor(ss);
        return processor.parseAgr(args);
    }
    
    /**
     * 命令行客户端执行入口
     *
     */
    public int executeDriver()
        throws IOException
    {
        if (ss.getFileName() != null)
        {
            try
            {
                return processFile(ss.getFileName());
            }
            catch (CQLException e)
            {
               LOG.error("Failed to process File {}", ss.getFileName());
               this.ss.getErr().println(e.getMessage());
               this.ss.getErr().flush();
            }
        }

        return -1;
    }
    
    private int readConconsole()
        throws IOException
    {
        ConsoleReader reader = new ConsoleReader();
        reader.setBellEnabled(false);
        //这样就可以支持'!='这样的输入了。
        reader.setExpandEvents(false);
        reader.setCopyPasteDetection(true);
        
        String line;
        int ret = 0;
        StringBuilder prefix = new StringBuilder();
        printHelp();
        
        String tip = DEFAULT_CLI_TIP;
        
        while ((line = reader.readLine(tip)) != null)
        {
            tip = WAITING_INPUT_TIP;
            if (!prefix.toString().equals(""))
            {
                prefix.append("\n");
            }
            
            if (line.trim().startsWith("--"))
            {
                continue;
            }
            
            if (line.trim().endsWith(";") && !line.trim().endsWith("\\;"))
            {
                line = prefix.toString() + line;
                ret = processLine(line);
                prefix.delete(0, prefix.length());
                tip = DEFAULT_CLI_TIP;
            }
            else
            {
                prefix.append(line);
                continue;
            }
        }
        return ret;
    }
    
    private int processLine(String line)
    {
        int lastRet = 0;
        int ret = 0;
        StringBuilder command = new StringBuilder();
        for (String oneCmd : line.split(";"))
        {
            if (StringUtils.isBlank(oneCmd.trim()))
            {
                continue;
            }
            
            if (StringUtils.endsWith(oneCmd, "\\"))
            {
                command.append(StringUtils.chop(oneCmd)).append(";");
                continue;
            }
            else
            {
                command.append(oneCmd);
            }
            
            if (StringUtils.isBlank(command.toString()))
            {
                continue;
            }
            
            ret = processCQL(command.toString(), true);
            lastRet = ret;
        }
        return lastRet;
    }
    
    private int processFile(String file)
        throws CQLException
    {
        LOG.debug("start to process cql file {}", file);
        List<String> sqls = CQLUtils.readCQLsFromFile(file);
        
        validateCQLFile(sqls);
        
        int ret = 0;
        for (int i = 0; i < sqls.size(); i++)
        {
            ret = processCQL(sqls.get(i), false);
        }
        
        return ret;
    }
    
    private void validateCQLFile(List<String> sqls)
        throws CQLException
    {
        for (int i = 0; i < sqls.size() - 1; i++)
        {
            String formattedSQL = sqls.get(i).toUpperCase(Locale.US).trim();
            if (formattedSQL.startsWith(SUBMIT_COMMAND))
            {
                CQLException exception = new CQLException(ErrorCode.TOP_ONE_FILE_ONE_TOP);
                LOG.error("Syntax error, too many submit clause.", exception);
                
                throw exception;
            }
        }
    }
    
    public int processCQL(String cql, boolean isConsole)
    {

        if (driver == null)
        {
            driver = new Driver(this);
        }

        try
        {
            driver.run(cql);
            printHeader();
            printResult();           
        }
        catch (CQLException e)
        {
            this.ss.getErr().println(e.getMessage());
            this.ss.getErr().flush();
            LOG.error("Failed to execute cql " + cql,e);
        }
        catch (Throwable t)
        {
            this.ss.getErr().println("Inner Error.");
            this.ss.getErr().flush();
            LOG.error("Failed to execute cql " + cql, t);
        }
        
        return 0;
    }



    public String checkCQL(String cql)
    {


        String result = "";

        if (driver == null)
        {
            driver = new Driver(this);
        }

        try
        {
            driver.run(cql);
            printHeader();
            printResult();
        }
        catch (CQLException e)
        {
            this.ss.getErr().println(e.getMessage());
            this.ss.getErr().flush();
            LOG.error("Failed to execute cql " + cql,e);
            result = ExceptionUtils.getStackTrace(e);
        }
        catch (Throwable t)
        {
            this.ss.getErr().println("Inner Error.");
            this.ss.getErr().flush();
            LOG.error("Failed to execute cql " + cql, t);
            result = ExceptionUtils.getStackTrace(t);
        }

        return result;
    }
    
    private void printHeader()
    {
        CQLResult result = driver.getResult();
        if (result == null)
        {
            return;
        }
        
        String[] heads = result.getHeads();
        if (heads.length == 0)
        {
            return;
        }
        
        this.ss.getOut().println(formatOutput(result.getFormatter(), heads));
        this.ss.getOut().flush();
    }
    
    private String formatOutput(String formatter, Object[] output)
    {
        if (Strings.isNullOrEmpty(formatter))
        {
            return Joiner.on("    ,    ").join(output);
        }
        
        return String.format(formatter, output);
    }
    
    private void printResult()
    {
        CQLResult cqlresult = driver.getResult();
        if (cqlresult == null)
        {
            return;
        }
        List<String[]> results = cqlresult.getResults();
        if (results == null || results.size() == 0)
        {
            return;
        }
        
        for (String[] heads : results)
        {
            if (heads == null || heads.length == 0)
            {
                continue;
            }
            this.ss.getOut().println(formatOutput(cqlresult.getFormatter(), heads));
        }
        this.ss.getOut().flush();
    }
    
    private void printHelp()
    {
        ss.getInfo().println(" End CQL with ';' and end client with 'exit;' ");
        ss.getInfo().flush();
    }
    
    /**
     * 在用户目录下创建临时文件.inputrc，用于屏蔽掉在suse环境下控制台打印警告信息
     */

    public void createTempFile()
    {
        String userHome =  System.getProperty("user.home");
        File file = new File(userHome, TEMP_FILE_FOR_SUSE);

        if (!file.exists())
        {
            try
            {
                if (file.createNewFile())
                {
                    file.deleteOnExit();
                }
                else
                {
                    LOG.warn("Create temp file " + TEMP_FILE_FOR_SUSE + ", but it doesn't effect CQL client.");
                }
            }
            catch (IOException e)
            {
                LOG.warn("Failed to create temp file " + TEMP_FILE_FOR_SUSE);
            }
        }
    }
    
    /**
     * CQL使用了logback的日志
     * 但是内部同样引用了log4j的jar包，
     * 所以在使用zookeeper进行安全登录的时候，控制台会打印两行日志：
     * log4j:WARN No appenders could be found for logger (org.apache.zookeeper.Login).
     * log4j:WARN Please initialize the log4j system properly
     * 所以要屏蔽这个日志。
     */
    public static void disableLog4j()
    {
        LOG.info("try to disable log4j console print.");
        Class< ? > logManagerClass = null;
        try
        {
            logManagerClass = Class.forName("org.apache.log4j.LogManager");
        }
        catch (ClassNotFoundException e)
        {
            //找不到该类，说明没有引用该log4j的jar包，所以日志自然不会打印，忽略即可。
            return;
        }
        
        try
        {
            Field selectorField = logManagerClass.getDeclaredField("repositorySelector");
            selectorField.setAccessible(true);
            DefaultRepositorySelector selector = (DefaultRepositorySelector)selectorField.get(null);
            
            Field repositoryField = selector.getClass().getDeclaredField("repository");
            repositoryField.setAccessible(true);
            LoggerRepository repository = (LoggerRepository)repositoryField.get(selector);
            
            if (!(repository instanceof Hierarchy))
            {
                return;
            }
            
            Hierarchy hierarchy = (Hierarchy)repository;
            Field emitted = hierarchy.getClass().getDeclaredField("emittedNoAppenderWarning");
            emitted.setAccessible(true);
            emitted.set(hierarchy, true);
        }
        catch (NoSuchFieldException e)
        {
            LOG.warn("Failed to disable log4j log print, NoSuchFieldException occurred.");
        }
        catch (SecurityException e)
        {
            LOG.warn("Failed to disable log4j log print, SecurityException occurred.");
        }
        catch (IllegalArgumentException e)
        {
            LOG.warn("Failed to disable log4j log print, IllegalArgumentException occurred.");
        }
        catch (IllegalAccessException e)
        {
            LOG.warn("Failed to disable log4j log print, IllegalAccessException occurred.");
        }
    }

    /**
     * 返回SessionState
     *
     * @return Value for property 'ss'.
     */
    public CQLSessionState getSs() {
        return ss;
    }

    /**
     * 返回用户自定义配置信息
     *
     * @return Value for property 'customizedConfigurationMap'.
     */
    public Map<String, Object> getCustomizedConfigurationMap() {
        return customizedConfigurationMap;
    }

    /**
     * 设置用户自定义配置信息
     *
     * @param customizedConfigurationMap .
     */
    public void setCustomizedConfigurationMap(Map<String, Object> customizedConfigurationMap) {
        this.customizedConfigurationMap = customizedConfigurationMap;
    }
}
