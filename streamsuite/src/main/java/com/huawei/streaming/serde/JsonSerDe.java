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

package com.huawei.streaming.serde;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.exception.StreamSerDeException;
import com.huawei.streaming.exception.StreamingException;

/**
 *  使用json
 * 
 * 使用配置的分隔符拆分消息
 * 
 */
public class JsonSerDe extends BaseSerDe
{
    private static final Logger LOG = LoggerFactory.getLogger(JsonSerDe.class);
    
    private static final long serialVersionUID = -2364817027725796314L;
    
    private List<Object[]> nullResults = Lists.newArrayList();

    public static final String LINE_SEPARATOR_UNIX = "\n";


    private StringBuilder sb = new StringBuilder();
    
    /**
     * {@inheritDoc}
     */
    @Override
    public void setConfig(StreamingConfig conf)
        throws StreamingException
    {
        super.setConfig(conf);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public List<Object[]> deSerialize(Object data)
        throws StreamSerDeException
    {
        if (data == null)
        {
            LOG.debug("Input raw data is null.");
            return nullResults;
        }
        
        String sData = data.toString();
        //空字符串当作null处理，这样才可以保证is null判断的正确性
        if (Strings.isNullOrEmpty(sData))
        {
            LOG.debug("Input raw data is null.");
            return nullResults;
        }

        String[] attributeNameArray = this.getSchema().getAllAttributeNames();

        List<Object[]> splitResults = Lists.newArrayList();



        Object object = JSONObject.parse(sData);

        if(object instanceof JSONObject){
            dealJSONObject(splitResults,(JSONObject)object,attributeNameArray);
        }
        else if(object instanceof JSONArray){

            JSONArray jsonArray = (JSONArray)object;
            int length = jsonArray.size();

            for(int i = 0;i<length;i++) {
                String jsonObjectStr = jsonArray.getString(i);
                JSONObject jsonObject = JSON.parseObject(jsonObjectStr);
                dealJSONObject(splitResults,jsonObject,attributeNameArray);
            }
        }
        return createAllInstance(splitResults);
    }

    private void dealJSONObject(List<Object[]> splitResults, JSONObject jsonObject, String[] attributeNameArray){


        Object[] values = new Object[attributeNameArray.length];

        Map<String,JSONObject> jsonObjectMap = new HashMap<String,JSONObject>();

        for(int j = 0; j<attributeNameArray.length;j++) {
            try {
                String attributeName = attributeNameArray[j];

                /***
                 * 属性名称预处理
                 * 因为 列名默认全小写,然后json中有的属性中有字母为大写 ,
                 * 所以 对于 myName 这种json属性设置为 my__u__name ,然后转换为 myName
                 */
                attributeName =  preDealAttributeName(attributeName);

                String[] attributeNames = attributeName.split("___"); //用 ___ 来分隔 json中的  父属性 和 子 jsonobject

                if (attributeNames.length == 1) {
                    values[j] = jsonObject.getString(attributeNames[0]);
                } else if (attributeNames.length == 2) {

                    JSONObject jsonObject1 = jsonObjectMap.get(attributeNames[0]);
                    if(jsonObject1 == null){
                        String value = jsonObject.getString(attributeNames[0]);
                        jsonObject1 = JSONObject.parseObject(value);
                        jsonObjectMap.put(attributeNames[0],jsonObject1);
                    }

                    values[j] = jsonObject1.getString(attributeNames[1]);
                }
            }catch(Exception e){
                LOG.error("处理jsonArraySerde 解码失败",e);
                values[j] = "";
            }
        }

        splitResults.add(values);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    public Object serialize(List<Object[]> event)
        throws StreamSerDeException
    {
        if (event == null)
        {
            LOG.info("Input event is null.");
            return null;
        }

        sb.delete(0, sb.length());
        for (int i = 0; i < event.size(); i++)
        {
            Object[] vals = event.get(i);
            String result = lineSerialize(vals);
            if (result != null)
            {
                sb.append(result + LINE_SEPARATOR_UNIX);
            }
        }

        String result =  sb.substring(0, sb.length() - LINE_SEPARATOR_UNIX.length());


        return result;
    }
    
    private String lineSerialize(Object[] vals)
    {
        String[] result = null;
        try
        {
            result = serializeRowToString(vals);
        }
        catch (StreamSerDeException e)
        {
            LOG.warn("One line is ignore.");
            return null;
        }

        Class[] types = this.getSchema().getAllAttributeTypes();
        String[] attributeNames = this.getSchema().getAllAttributeNames();

        JSONObject jsonObject = new JSONObject();

        for (int i = 0; i < result.length; i++)
        {

            String attributeName =  preDealAttributeName(attributeNames[i]);

            if(types[i] == Boolean.class ) {
                jsonObject.put(attributeName, Boolean.valueOf(result[i]));
            }
            else if(types[i] == Integer.class ) {
                jsonObject.put(attributeName, Integer.valueOf(result[i]));
            }
            else if(types[i] == Long.class ) {
                jsonObject.put(attributeName, Long.valueOf(result[i]));
            }
            else if(types[i] == Float.class ) {
                jsonObject.put(attributeName, Float.valueOf(result[i]));
            }
            else if(types[i] == Double.class ) {
                jsonObject.put(attributeName, Double.valueOf(result[i]));
            }
            else {
                jsonObject.put(attributeName, String.valueOf(result[i]));
            }

        }
        String resultStr =  jsonObject.toJSONString();

        return resultStr;
    }


    /***
     * 属性名称预处理
     * 因为 列名默认全小写,然后json中有的属性中有字母为大写 ,
     * 所以 对于 myName 这种json属性设置为 my__u__name ,然后转换为 myName
     */
    private final String TAG_UPPER = "__u__";
    private String preDealAttributeName(String attributeName) {
        String[] attributeNames = attributeName.split(TAG_UPPER);

        String result = attributeNames[0];

        for(int i = 1;i<attributeNames.length;i++){

            char[] cs=attributeNames[i].toCharArray();
            cs[0]= String.valueOf(cs[0]).toUpperCase().toCharArray()[0];

            result += String.valueOf(cs);
        }

        return result;
    }




}
