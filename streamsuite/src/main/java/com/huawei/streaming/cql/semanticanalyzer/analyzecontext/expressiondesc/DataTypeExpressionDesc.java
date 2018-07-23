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

package com.huawei.streaming.cql.semanticanalyzer.analyzecontext.expressiondesc;

import com.huawei.streaming.cql.semanticanalyzer.ConstUtils;

/**
 * 数据类型描述
 * 
 */
public class DataTypeExpressionDesc implements ExpressionDescribe
{
    /**
     * 数据类型
     */
    private Class< ? > type;
    
    /**
     * <默认构造函数>
     */
    public DataTypeExpressionDesc(Class< ? > type)
    {
        super();
        this.type = type;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return ConstUtils.getDataType(type);
    }
    
    public Class< ? > getType()
    {
        return type;
    }
    
    public void setType(Class< ? > type)
    {
        this.type = type;
    }
}
