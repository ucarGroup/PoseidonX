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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.google.common.base.Joiner;
import com.huawei.streaming.cql.executor.FunctionInfo;
import com.huawei.streaming.cql.executor.FunctionType;
import com.huawei.streaming.cql.executor.expressioncreater.FunctionExpressionCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.ExpressionCreatorAnnotation;

/**
 * 所有函数类的描述类
 * 
 */

@ExpressionCreatorAnnotation(FunctionExpressionCreator.class)
public class FunctionExpressionDesc implements ExpressionDescribe
{
    /**
     * 函数中是否有distinct
     */
    private boolean isDistinct = false;
    
    /**
     * 是否包含*号
     * 即是否有count(*) 
     */
    private boolean isSelectStar = false;
    
    /**
     * 函数信息
     * 包含函数名称，函数类型等
     */
    private FunctionInfo finfo;
    
    /**
     * 输出列的别名，
     * 仅在udtf函数的时候有用，
     * 因为只有udtf函数不会和别的表达式嵌套使用
     */
    private String[] resultColumnAlias;
    
    /**
     * 函数的参数列表
     */
    private List<ExpressionDescribe> argExpressions = new ArrayList<ExpressionDescribe>();;
    
    /**
     * <默认构造函数>
     */
    public FunctionExpressionDesc(FunctionInfo finfo)
    {
        super();
        this.finfo = finfo;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();
        
        sb.append(" " + finfo.getName() + "( ");

        if (isDistinct)
        {
            sb.append("DISTINCT ");
        }

        if (isSelectStar)
        {
            sb.append("*");
        }

        sb.append(Joiner.on(" ,").join(argExpressions));

        sb.append(" ) ");
        
        if (finfo.getType().equals(FunctionType.UDTF))
        {
            sb.append(" AS ");
            sb.append(" ( ");
            
            for (int i = 0; i < resultColumnAlias.length; i++)
            {
                sb.append(resultColumnAlias[i]);
                if (i != resultColumnAlias.length - 1)
                {
                    sb.append(", ");
                }
            }
            
            sb.append(" ) ");
            
        }
        
        return sb.toString();
    }
    
    public boolean isDistinct()
    {
        return isDistinct;
    }
    
    public void setDistinct(boolean distinct)
    {
        this.isDistinct = distinct;
    }
    
    public List<ExpressionDescribe> getArgExpressions()
    {
        return argExpressions;
    }
    
    public void setArgExpressions(List<ExpressionDescribe> argExpressions)
    {
        this.argExpressions = argExpressions;
    }
    
    public FunctionInfo getFinfo()
    {
        return finfo;
    }
    
    public void setFinfo(FunctionInfo finfo)
    {
        this.finfo = finfo;
    }
    
    public boolean isSelectStar()
    {
        return isSelectStar;
    }
    
    public void setSelectStar(boolean selectStar)
    {
        this.isSelectStar = selectStar;
    }
    
    public String[] getResultColumnAlias()
    {
        return resultColumnAlias == null ? new String[] {} : (String[])resultColumnAlias.clone();
    }
    
    public void setResultColumnAlias(String[] resultColumnAlias)
    {
        this.resultColumnAlias = Arrays.copyOf(resultColumnAlias, resultColumnAlias.length);;
    }
    
}
