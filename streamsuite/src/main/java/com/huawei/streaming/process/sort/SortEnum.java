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
/*
 * 文 件 名:  OrderByEnum.java
 * 版 本 号:  V1.0.0
 * 版    权:  Huawei Technologies Co., Ltd. Copyright 1988-2008,  All rights reserved
 * 描    述:  <描述>
 * 作    者:  z00221388
 * 创建日期:  2013-6-13
 */
package com.huawei.streaming.process.sort;

/**
 * 排序的枚举值，包括升序、降序
 */
public enum SortEnum
{
    /**
     * 升序
     */
    ASC("ASC"),
    
    /**
     * 降序
     */
    DESC("DESC");
    
    private String desc;
    
    SortEnum(String d)
    {
        this.desc = d;
    }
    
    public String getDesc()
    {
        return desc;
    }
}
