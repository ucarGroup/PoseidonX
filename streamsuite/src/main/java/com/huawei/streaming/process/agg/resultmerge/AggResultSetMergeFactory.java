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

package com.huawei.streaming.process.agg.resultmerge;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;

import com.huawei.streaming.common.MultiKey;
import com.huawei.streaming.expression.AggregateExpression;
import com.huawei.streaming.expression.AggregateGroupedExpression;
import com.huawei.streaming.expression.IExpression;
import com.huawei.streaming.process.GroupBySubProcess;
import com.huawei.streaming.process.LimitProcess;
import com.huawei.streaming.process.OrderBySubProcess;
import com.huawei.streaming.process.SelectSubProcess;
import com.huawei.streaming.process.agg.compute.IAggregationService;

/**
 * <属性和聚合操作的结果合并处理工厂类>
 * <功能详细描述>
 * 
 */
public class AggResultSetMergeFactory
{
    
    private static HashMap<MultiKey, Class< ? >> aggResultSetMerges;
    
    static
    {
        aggResultSetMerges = new HashMap<MultiKey, Class< ? >>();
        
        aggResultSetMerges.put(new MultiKey(new Object[] {true, true}), AggResultSetMergeOnlyGrouped.class);
        aggResultSetMerges.put(new MultiKey(new Object[] {true, false}), AggResultSetMergeOnly.class);
        aggResultSetMerges.put(new MultiKey(new Object[] {false, true}), AggResultSetMergeGrouped.class);
        aggResultSetMerges.put(new MultiKey(new Object[] {false, false}), AggResultSetMerge.class);
    }
    
    /**
     * <创建结果合并处理类>
     * <功能详细描述>
     */
    public static IResultSetMerge makeAggResultSetMerge(IAggregationService aggregator, SelectSubProcess selector,
        GroupBySubProcess groupby, OrderBySubProcess order, LimitProcess limit)
    {
        if (selector == null || aggregator == null)
        {
            String msg = "param is null";
            throw new IllegalArgumentException(msg);
        }
        
        boolean isOnlyAgg = detectOnlyAgg(selector);
        
        boolean isGrouped = aggregator.isGrouped();
        
        Class< ? > aggResultSetMergeClass = aggResultSetMerges.get(new MultiKey(new Object[] {isOnlyAgg, isGrouped}));
        
        Constructor< ? > con;
        try
        {
            con =
                aggResultSetMergeClass.getConstructor(IAggregationService.class,
                    SelectSubProcess.class,
                    GroupBySubProcess.class,
                    OrderBySubProcess.class,
                    LimitProcess.class);
            
            return (IResultSetMerge)con.newInstance(aggregator, selector, groupby, order, limit);
        }
        catch (SecurityException e)
        {
            throw new RuntimeException(e);
        }
        catch (NoSuchMethodException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalArgumentException e)
        {
            throw new RuntimeException(e);
        }
        catch (InstantiationException e)
        {
            throw new RuntimeException(e);
        }
        catch (IllegalAccessException e)
        {
            throw new RuntimeException(e);
        }
        catch (InvocationTargetException e)
        {
            throw new RuntimeException(e);
        }
    }
    
    /**
     * <检测select子句中是否只包含聚合操作表达式>
     * <功能详细描述>
     */
    private static boolean detectOnlyAgg(SelectSubProcess selector)
    {
        IExpression[] exprs = selector.getExprs();
        for (int i = 0; i < exprs.length; i++)
        {
            if (!(exprs[i] instanceof AggregateExpression) && !(exprs[i] instanceof AggregateGroupedExpression))
            {
                return false;
            }
        }
        
        return true;
    }
}
