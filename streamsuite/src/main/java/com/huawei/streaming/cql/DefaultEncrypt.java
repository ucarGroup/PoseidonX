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

import com.huawei.streaming.exception.StreamingException;

import java.io.Console;
import java.io.IOException;
import java.util.Arrays;

/**
 * 默认的加密类
 *
 */
public class DefaultEncrypt
{
    private static final String DEFAULT_CLI_TIP = "Please enter encrypt text :";

    public static void main(String[] args) throws IOException, StreamingException
    {
        String encodeText = encrypt();
        System.out.println(encodeText);
    }

    private static String encrypt() throws IOException, StreamingException
    {
        Console console = System.console();
        if (console == null) {
            throw new IOException("Can not get System console. maybe it running in an IDE.");
        }

        char[] passwordArray = console.readPassword(DEFAULT_CLI_TIP);
        if(passwordArray == null || passwordArray.length == 0)
        {
            throw new IOException("User input can not be null.");
        }

        try
        {
            return new com.huawei.streaming.encrypt.NoneEncrypt().encrypt(new String(passwordArray));
        }
        finally
        {
            //密码使用结束之后清空密码数组
            Arrays.fill(passwordArray, ' ');
        }
    }
}
