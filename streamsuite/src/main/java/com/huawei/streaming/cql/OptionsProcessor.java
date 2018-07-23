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

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 命令行客户端参数处理类
 * 
 */
public class OptionsProcessor
{
    private static final Logger LOG = LoggerFactory.getLogger(OptionsProcessor.class);
    
    private final Options options = new Options();
    
    private CQLSessionState ss;
    
    /**
     * <默认构造函数>
     */
    public OptionsProcessor(CQLSessionState sessionState)
    {
        this.ss = sessionState;
        initOptions();
    }
    
    @SuppressWarnings("static-access")
    private void initOptions()
    {
        // -f <query-file>
        options.addOption(OptionBuilder.hasArg()
            .withLongOpt("file")
            .withArgName("filename")
            .withDescription("CQL from files")
            .create('f'));
        
        // [-S|--silent]
        options.addOption(new Option("s", "silent", false, "Silent mode in interactive shell"));
        
        // [-H|--help]
        options.addOption(new Option("h", "help", false, "Print help information"));
    }
    
    /**
     * 解析参数
     */
    public boolean parseAgr(String[] args)
    {
        CommandLine commandLine = null;
        try
        {
            commandLine = new GnuParser().parse(options, args);
        }
        catch (ParseException e)
        {
            LOG.error("Command line option args error.");
            return false;
        }
        
        if (commandLine.hasOption('h'))
        {
            printUsage();
            return false;
        }
        
        ss.setSilent(commandLine.hasOption('s'));
        ss.setFileName(commandLine.getOptionValue('f'));
        return true;
    }
    
    private void printUsage()
    {
        new HelpFormatter().printHelp("streaming", options);
    }
}
