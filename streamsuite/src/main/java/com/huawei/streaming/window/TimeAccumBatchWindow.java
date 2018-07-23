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

package com.huawei.streaming.window;

import java.math.BigDecimal;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.alibaba.fastjson.JSONObject;
import com.huawei.streaming.api.opereators.Window;
import com.huawei.streaming.view.IDataCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.event.IEvent;
import com.huawei.streaming.timeservice.TimeService;

/**
 * <累计求和 时间跳动窗口实现类>
 * 
 */
public class TimeAccumBatchWindow extends TimeBasedWindow implements IBatch
{
    /**
     * 序列化ID
     */
    private static final long serialVersionUID = -3916243765038851148L;

    /**
     * 日志打印对象
     */
    private static final Logger LOG = LoggerFactory.getLogger(TimeAccumBatchWindow.class);


    /*****
     * 缓存的统计数据
     */
     private ConcurrentHashMap<String,IEvent> batchInfo = new ConcurrentHashMap<String, IEvent>();


     private IEvent[] oldData;

     private List<String> groupNames;
     private List<String> sumColNames;

    /**
     * <默认构造函数>
     * @param window 窗口描述信息
     */
    public TimeAccumBatchWindow(Window window)
    {
        super(window.getLength());
        LOG.error("###### TimeAccumBatchWindow init["+JSONObject.toJSONString(window)+"]");
        TimeService timeservice = new TimeService(window.getLength(), this);
        setTimeservice(timeservice);

        String groupNameStr = window.getGroupbyExpression();

        if(groupNameStr == null || groupNameStr.length() == 0){
            this.groupNames = new ArrayList<String>();
        }
        else{
            this.groupNames = new ArrayList<String>();

            String[] groupNameArray = groupNameStr.split(",");

            for(String groupName:groupNameArray){

                int index = groupName.indexOf(".");
                if(index == -1 ) {
                    this.groupNames.add(groupName);
                }
                else{
                    this.groupNames.add(groupName.substring(index+1));
                }
            }
        }

        String outColNameStr = window.getOutputExpression();

        String[] outColNames =  outColNameStr.split(",");

        sumColNames = new ArrayList<String>();

        for(String outColName : outColNames){
            outColName = outColName.replaceAll(" ","");
              if(outColName.startsWith("sum(")){
                  String tmpCol = outColName.replace("sum(","").replace(")","");
                  int index = tmpCol.indexOf(".");
                  if(index == -1 ) {
                      this.sumColNames.add(tmpCol);
                  }
                  else{
                      this.sumColNames.add(tmpCol.substring(index+1));
                  }
              }
        }

        LOG.error("####### groupNames ["+JSONObject.toJSONString(groupNames)+"]");
        LOG.error("####### sumColNames ["+JSONObject.toJSONString(sumColNames)+"]");

    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void update(IEvent[] newData, IEvent[] oldData)
    {
        //TODO 未考虑窗口叠加时，上一个窗口传递的过期数据的处理。
        
        if ((null == newData) || (newData.length == 0))
        {
            LOG.error("Input Time Batch Window newData is Null!");
            return;
        }
        
        try
        {
            lock();

            for (IEvent event : newData)
            {
                Object[] currentValues = event.getAllValues();
                String[] attributeNames = event.getEventType().getAllAttributeNames();


                String groupStr = "KEY";

                for(int i = 0;i<attributeNames.length;i++){

                    String attributeName = attributeNames[i];

                    if(groupNames.contains(attributeName)){
                        groupStr += String.valueOf(currentValues[i]);
                    }
                }

                IEvent batchEvent = batchInfo.get(groupStr);


                if(batchEvent == null){
                    batchEvent = event;
                    batchInfo.put(groupStr,batchEvent);
                }
                else{
                Object[] batchValues = batchEvent.getAllValues();
                Class[] attributeTypes = batchEvent.getEventType().getAllAttributeTypes();

                for(int i = 0;i<attributeNames.length;i++) {
                    String attributeName = attributeNames[i];

                    if (sumColNames.size() > 0 && sumColNames.contains(attributeName)) {
                        Class clazz = attributeTypes[i];


                        //int
                        if (clazz == Integer.class) {
                            batchValues[i] = ((Integer) batchValues[i]) + ((Integer) currentValues[i]);
                        }
                        //long
                        else if (clazz == Long.class) {
                            batchValues[i] = ((Long) batchValues[i]) + ((Long) currentValues[i]);
                        }
                        //decimal
                        else if (clazz == BigDecimal.class) {
                            batchValues[i] = ((BigDecimal) batchValues[i]).add((BigDecimal) currentValues[i]);
                        }
                    }
                }

                }
            }
        }catch (Exception e){
            LOG.error("update error",e);
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void timerCallBack(long currentTime)
    {
        //TODO 加锁操作对性能的影响，有没有更好的方法？
        try
        {
            lock();
            sendBatchData();
        }
        finally
        {
            if (isLocked())
            {
                unlock();
            }
        }
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void sendBatchData()
    {
        if (this.hasViews())
        {
            IEvent[] newData = null;
            if (!batchInfo.isEmpty())
            {
                newData = batchInfo.values().toArray(new IEvent[batchInfo.size()]);
                batchInfo.clear();
            }

            // 清空上一次记录
            oldData = new IEvent[0];



            if ((newData != null) || (oldData != null))
            {
                IDataCollection dataCollection = getDataCollection();
                if (dataCollection != null)
                {
                    dataCollection.update(newData, oldData);
                }

                updateChild(newData, oldData);
            }
        }



    }
}
