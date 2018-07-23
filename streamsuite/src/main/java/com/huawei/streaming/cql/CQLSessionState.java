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

import java.io.InputStream;
import java.io.PrintStream;

/**
 * 用户session
 * 
 */
public class CQLSessionState
{
    /**
     * 正常状态
     */
    public static final int STATE_OK = 0;
    
    /**
     * 未知错误
     */
    public static final int STATE_UNKOWN_ERROR = 1;
    
    /**
     * 正常退出
     */
    public static final int STATE_NORMAL_EXIT = 2;
    
    /**
     * 命令执行异常
     */
    public static final int STATE_CMD_ERROR = 3;
    
    /**
     * 是否是静默模式
     */
    private boolean isSilent = false;
    
    private InputStream in;
    
    private PrintStream out;
    
    private PrintStream info;
    
    private PrintStream err;
    
    private String fileName;
    
    /**
     * 关闭session
     */
    public void close()
    {
        
    }
    
    public boolean isSilent()
    {
        return isSilent;
    }
    
    public void setSilent(boolean issilent)
    {
        this.isSilent = issilent;
    }
    
    public InputStream getIn()
    {
        return in;
    }
    
    public void setIn(InputStream in)
    {
        this.in = in;
    }
    
    public PrintStream getOut()
    {
        return out;
    }
    
    public void setOut(PrintStream out)
    {
        this.out = out;
    }
    
    public PrintStream getInfo()
    {
        return info;
    }
    
    public void setInfo(PrintStream info)
    {
        this.info = info;
    }
    
    public PrintStream getErr()
    {
        return err;
    }
    
    public void setErr(PrintStream err)
    {
        this.err = err;
    }
    
    public String getFileName()
    {
        return fileName;
    }
    
    public void setFileName(String fileName)
    {
        this.fileName = fileName;
    }
}
