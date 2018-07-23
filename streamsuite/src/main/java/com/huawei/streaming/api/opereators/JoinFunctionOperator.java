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

package com.huawei.streaming.api.opereators;

import com.huawei.streaming.cql.executor.operatorinfocreater.JoinInfoOperatorCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * 进行join操作的算子。
 * 
 * TODO： 表达式中，a.id，b.id 这个a，b都是流名称
 * 所以，表达式的解析，要重新修改下。
 * 
 */
@OperatorInfoCreatorAnnotation(JoinInfoOperatorCreator.class)
public class JoinFunctionOperator extends BasicAggFunctionOperator
{
    
    /**
     * 左流名称
     */
    private String leftStreamName;
    
    /**
     * 右流名称
     */
    private String rightStreamName;
    
    /** 
     * 左窗口
     */
    private Window leftWindow;
    
    /**
     * 右窗口
     */
    private Window rightWindow;
    
    /**
     * join类型
     */
    private JoinType joinType;
    
    /**
     * join条件 
     */
    private String joinExpression;
    
    /**
     * join之后的过滤条件
     */
    private String filterAfterJoinExpression;
    
    /**
     * 单向输出流索引
     */
    private UniDiRectionType uniDirectionIndex = UniDiRectionType.NONE_STREAM;
    
    /**
     * <默认构造函数>
     */
    public JoinFunctionOperator(String id, int parallelNumber)
    {
        super(id, parallelNumber);
    }
    
    public Window getLeftWindow()
    {
        return leftWindow;
    }
    
    public void setLeftWindow(Window leftWindow)
    {
        this.leftWindow = leftWindow;
    }
    
    public Window getRightWindow()
    {
        return rightWindow;
    }
    
    public void setRightWindow(Window rightWindow)
    {
        this.rightWindow = rightWindow;
    }
    
    public JoinType getJoinType()
    {
        return joinType;
    }
    
    public void setJoinType(JoinType joinType)
    {
        this.joinType = joinType;
    }
    
    public String getLeftStreamName()
    {
        return leftStreamName;
    }
    
    public void setLeftStreamName(String leftStreamName)
    {
        this.leftStreamName = leftStreamName;
    }
    
    public String getRightStreamName()
    {
        return rightStreamName;
    }
    
    public void setRightStreamName(String rightStreamName)
    {
        this.rightStreamName = rightStreamName;
    }
    
    public String getJoinExpression()
    {
        return joinExpression;
    }
    
    public void setJoinExpression(String joinExpression)
    {
        this.joinExpression = joinExpression;
    }
    
    public String getFilterAfterJoinExpression()
    {
        return filterAfterJoinExpression;
    }
    
    public void setFilterAfterJoinExpression(String filterAfterJoinExpression)
    {
        this.filterAfterJoinExpression = filterAfterJoinExpression;
    }
    
    public UniDiRectionType getUniDirectionIndex()
    {
        return uniDirectionIndex;
    }
    
    public void setUniDirectionIndex(UniDiRectionType uniDirectionIndex)
    {
        this.uniDirectionIndex = uniDirectionIndex;
    }
}
