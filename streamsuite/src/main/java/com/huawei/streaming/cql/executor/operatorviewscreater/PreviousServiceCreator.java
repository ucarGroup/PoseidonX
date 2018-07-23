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

package com.huawei.streaming.cql.executor.operatorviewscreater;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.cql.exception.ExecutorException;
import com.huawei.streaming.cql.executor.WindowInfo;
import com.huawei.streaming.cql.executor.WindowRegistry;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.expression.PreviousExpression;
import com.huawei.streaming.view.IDataCollection;
import com.huawei.streaming.window.AbstractAccessService;
import com.huawei.streaming.window.IWindow;
import com.huawei.streaming.window.RandomAccessByIndexService;
import com.huawei.streaming.window.RelativeAccessByEventAndIndexService;
import com.huawei.streaming.window.WindowRandomAccess;
import com.huawei.streaming.window.WindowRelativeAccess;

/**
 * previouservice创建类
 * 
 * previous和流以及窗口绑定
 * previous要求该流只有一个窗口
 * 
 * 所有的batch窗口都创建RelativeService。RelativeAccessByEventAndIndexService
 * 所有的slide窗口都创建random的service。RandomAccessByIndexService
 * 
 */
public class PreviousServiceCreator
{
    private static final Logger LOG = LoggerFactory.getLogger(PreviousServiceCreator.class);
    
    /**
     * 创建prievious表达式并且设置在window以及表达式中
     */
    public void createAndSet(IWindow win, List<PreviousExpression> previousInSameStream)
        throws ExecutorException
    {
        AbstractAccessService service = create(win);
        for (PreviousExpression exp : previousInSameStream)
        {
            exp.setService(service);
        }
    }
    
    private AbstractAccessService create(IWindow win)
        throws ExecutorException
    {
        Boolean isSlide = isSlideWindow(win);
        if (isSlide)
        {
            return createSlideService(win);
        }
        
        return createBatchService(win);
    }
    
    private AbstractAccessService createBatchService(IWindow win)
    {
        RelativeAccessByEventAndIndexService service = new RelativeAccessByEventAndIndexService();
        IDataCollection access = new WindowRelativeAccess(service);
        win.setDataCollection(access);
        return service;
    }
    
    private AbstractAccessService createSlideService(IWindow win)
    {
        RandomAccessByIndexService service = new RandomAccessByIndexService();
        IDataCollection access = new WindowRandomAccess(service);
        win.setDataCollection(access);
        return service;
    }
    
    private Boolean isSlideWindow(IWindow win)
        throws ExecutorException
    {
        String winName = WindowRegistry.getWindowNameByClass(win.getClass());
        if (null == winName)
        {
            ExecutorException exception =
                new ExecutorException(ErrorCode.SEMANTICANALYZE_UNKOWN_CLASS, win.getClass().getName());
            LOG.error("Faild to get windown name.", exception);
            throw exception;
        }
        
        WindowInfo winfo = WindowRegistry.getWindowInfo(winName);
        Boolean isSlide = winfo.getIsSlide();
        if (isSlide == null)
        {
            ExecutorException exception = new ExecutorException(ErrorCode.WINDOW_UNRECGNIZE_WINDOW);
            LOG.error("Unknown window.", exception);
            throw exception;
        }
        return isSlide;
    }
}
