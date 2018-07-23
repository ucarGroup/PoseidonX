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

import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.huawei.streaming.cql.executor.windowcreater.WindowCreator;
import com.huawei.streaming.window.IWindow;

/**
 * 窗口信息
 *
 */
public class WindowInfo
{
    private String widowName;

    private boolean isNative = false;

    private Class< ? extends IWindow> instanceClass = null;

    private Class< ? extends WindowCreator> createrClass = null;

    private List<String> args = null;

    /**
     * 是否是滑动窗口
     * 如果是用户自定义窗口，一定是null
     * 系统内部的窗口，都是已经定义好的
     */
    private Boolean isSlide = null;

    /**
     * <默认构造函数>
     */
    public WindowInfo(String winName, Class< ? extends IWindow> instantce, Class< ? extends WindowCreator> creater,
        String args)
    {
        this.widowName = winName;
        List<String> windowArgs = null;
        if (!StringUtils.isEmpty(args))
        {
            String[] argarr = args.trim().split(",");
            windowArgs = Arrays.asList(argarr);
        }

        this.instanceClass = instantce;
        this.createrClass = creater;
        this.args = windowArgs;

    }

    public Class< ? extends IWindow> getInstanceClass()
    {
        return instanceClass;
    }

    public void setInstanceClass(Class< ? extends IWindow> instanceClass)
    {
        this.instanceClass = instanceClass;
    }

    public Class< ? extends WindowCreator> getCreatorClass()
    {
        return createrClass;
    }

    public void setCreatorClass(Class< ? extends WindowCreator> creatorClass)
    {
        this.createrClass = creatorClass;
    }

    public List<String> getArgs()
    {
        return args;
    }

    public void setArgs(List<String> args)
    {
        this.args = args;
    }

    public boolean isNative()
    {
        return isNative;
    }

    public void setNative(boolean isnative)
    {
        this.isNative = isnative;
    }

    public String getWidowName()
    {
        return widowName;
    }

    public void setWidowName(String widowName)
    {
        this.widowName = widowName;
    }

    public Boolean getIsSlide()
    {
        return isSlide;
    }

    public void setIsSlide(Boolean isSlide)
    {
        this.isSlide = isSlide;
    }

}
