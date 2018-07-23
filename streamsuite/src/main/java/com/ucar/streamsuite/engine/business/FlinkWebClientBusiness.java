package com.ucar.streamsuite.engine.business;

import backtype.storm.generated.TopologySummary;
import backtype.storm.utils.NimbusClient;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.util.HttpSendClient;
import com.ucar.streamsuite.common.util.HttpSendClientFactory;
import com.ucar.streamsuite.common.util.YarnClientProxy;

import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.FlinkJobDTO;
import com.ucar.streamsuite.engine.dto.FlinkJobVerticeDTO;
import com.ucar.streamsuite.engine.dto.JstormTopologyDTO;
import com.ucar.streamsuite.moniter.dto.FlinkMetricDto;
import com.ucar.streamsuite.moniter.dto.FlinkVerticeMetricDto;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Description: flink 运行时环境的客户端，
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class FlinkWebClientBusiness {

    public static final Logger LOGGER = LoggerFactory.getLogger(FlinkWebClientBusiness.class);

    private static HttpSendClient instance = null;
    private static HttpSendClient infiniteInstance = null;
    static{
        instance = HttpSendClientFactory.getInstance();
        infiniteInstance = HttpSendClientFactory.getNTOInstance();
    }

    public static String buildYarnProxyAddress(String yarnAppId){
        if(StringUtils.isBlank(yarnAppId)){
            return "";
        }
        return "http://"+ YarnClientProxy.getActiveRMWebAddresses() + "/proxy/" + yarnAppId;
    }

    private static boolean isJson(String content){
        try {
            JSONObject.parseObject(content);
            return  true;
        } catch (Exception e) {
            return false;
        }
    }

    private static boolean isJsonArray(String content){
        try {
            JSONObject.parseArray(content);
            return  true;
        } catch (Exception e) {
            return false;
        }
    }

    public static String getYarnAppOveriew(String yarnAppId){
        String rs = "";
        try {
            if(StringUtils.isBlank(yarnAppId)){
                return "";
            }
            String address = buildYarnProxyAddress(yarnAppId) + "/overview/";
            rs = instance.get(address,null);
            if(!isJson(rs)){
                return "";
            }
        } catch (IOException e) {
        }
        return rs;
    }

    public static String getTaskManagers(String yarnAppId){
        String rs = "";
        try {
            if(StringUtils.isBlank(yarnAppId)){
                return "";
            }
            String address = buildYarnProxyAddress(yarnAppId) + "/taskmanagers/";
            rs = instance.get(address,null);
            if(!isJson(rs)){
                return "";
            }
        } catch (IOException e) {
        }
        return rs;
    }

    public static List<String> getTaskManagerDetail(String yarnAppId){
        List<String> taskManagerDetails = Lists.newArrayList();
        Set<String> taskManagerIds = Sets.newHashSet();

        String taskManagers = getTaskManagers(yarnAppId);
        if(StringUtils.isBlank(taskManagers)){
           return taskManagerDetails;
        }

        JSONObject taskManagersJSONObject  = JSONObject.parseObject(taskManagers);
        if(taskManagersJSONObject == null){
            return taskManagerDetails;
        }

        String taskmanagers = taskManagersJSONObject.getString("taskmanagers");
        if(StringUtils.isBlank(taskmanagers)){
            return taskManagerDetails;
        }

        JSONArray jSONArray = JSONObject.parseArray(taskmanagers);
        if(jSONArray!=null && jSONArray.size() > 0){
            int i=0;
            for(;i < jSONArray.size();){
                String id = jSONArray.getJSONObject(i).getString("id");
                if(StringUtils.isNotBlank(id)){
                    taskManagerIds.add(id);
                }
                i++;
            }
        }
        return getTaskManagerDetail(yarnAppId, taskManagerIds);
    }

    public static List<String> getTaskManagerDetail(String yarnAppId, Set<String> taskManagerIds){
        List<String> taskManagerDetails = Lists.newArrayList();
        if(StringUtils.isBlank(yarnAppId) || CollectionUtils.isEmpty(taskManagerIds)){
            return taskManagerDetails;
        }
        for(String taskManagerId:taskManagerIds){
            try {
                String address = buildYarnProxyAddress(yarnAppId) + "/taskmanagers/" + taskManagerId;
                String rs = instance.get(address,null);
                if(StringUtils.isNotBlank(rs) && isJson(rs)){
                    taskManagerDetails.add(rs);
                }
            } catch (IOException e) {
            }
        }
        return taskManagerDetails;
    }

    public static String getJobManagerConfig(String yarnAppId){
        String rs = "";
        try {
            if(StringUtils.isBlank(yarnAppId)){
                return "";
            }
            String address = buildYarnProxyAddress(yarnAppId) + "/jobmanager/config";
            rs = instance.get(address,null);
            if(!isJsonArray(rs)){
                return "";
            }
        } catch (IOException e) {
        }
        return rs;
    }

    public static List<String> getVerticeDetail(String yarnAppId, String jobId){
        List<String> verticeDetails = Lists.newArrayList();
        Set<String> verticeIds = getVerticeIds( yarnAppId,jobId);
        return  getVerticeDetail(yarnAppId,jobId,verticeIds);
    }

    public static Set<String> getVerticeIds(String yarnAppId, String jobId){
        Set<String> verticeIds = Sets.newHashSet();

        String jobDetail = getJobDetail(yarnAppId, jobId);
        if(StringUtils.isBlank(jobDetail)){
            return verticeIds;
        }

        JSONObject jobDetailJSONObject  = JSONObject.parseObject(jobDetail);
        if(jobDetailJSONObject == null){
            return verticeIds;
        }

        String vertices = jobDetailJSONObject.getString("vertices");
        if(StringUtils.isBlank(vertices)){
            return verticeIds;
        }

        JSONArray jSONArray = JSONObject.parseArray(vertices);
        if(jSONArray!=null && jSONArray.size() > 0){
            int i=0;
            for(;i < jSONArray.size();){
                String id = jSONArray.getJSONObject(i).getString("id");
                if(StringUtils.isNotBlank(id)){
                    verticeIds.add(id);
                }
                i++;
            }
        }
        return verticeIds;
    }

    public static List<FlinkJobVerticeDTO> getJobVerticeDTOs(String yarnAppId, String jobId){
        List<FlinkJobVerticeDTO> flinkJobVerticeDTOs = Lists.newArrayList();

        String jobDetail = getJobDetail(yarnAppId, jobId);
        if(StringUtils.isBlank(jobDetail)){
            return null;
        }

        JSONObject jobDetailJSONObject  = JSONObject.parseObject(jobDetail);
        if(jobDetailJSONObject == null){
            return null;
        }

        String vertices = jobDetailJSONObject.getString("vertices");
        if(StringUtils.isBlank(vertices)){
            return null;
        }

        flinkJobVerticeDTOs = JSONObject.parseArray(vertices,FlinkJobVerticeDTO.class);
        return flinkJobVerticeDTOs;
    }

    public static List<String> getVerticeDetail(String yarnAppId, String jobId, Set<String> verticeIds){
        List<String> verticeDetails = Lists.newArrayList();
        if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId) ||  CollectionUtils.isEmpty(verticeIds)){
            return verticeDetails;
        }
        for(String verticeId:verticeIds){
            try {
                String address = buildYarnProxyAddress(yarnAppId) + "/jobs/" + jobId + "/vertices/" + verticeId;
                String rs = instance.get(address,null);
                if(StringUtils.isNotBlank(rs) && isJson(rs)){
                    verticeDetails.add(rs);
                }
            } catch (IOException e) {
            }
        }
        return verticeDetails;
    }

    public static String getJobDetail(String yarnAppId, String jobId){
        String rs = "";
        try {
            if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
                return "";
            }
            String address = buildYarnProxyAddress(yarnAppId) + "/jobs/" + jobId;
            rs = instance.get(address,null);
            if(!isJson(rs)){
                return "";
            }
        } catch (IOException e) {
        }
        return rs;
    }

    public static String getJobException(String yarnAppId, String jobId){
        String rs = "";
        try {
            if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
                return "";
            }
            String address = buildYarnProxyAddress(yarnAppId) + "/jobs/" + jobId + "/exceptions";
            rs = instance.get(address,null);
            if(!isJson(rs)){
                return "";
            }
        } catch (IOException e) {
        }
        return rs;
    }

    public static String getJobRootException(String yarnAppId, String jobId){
        if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
            return "";
        }
        String jobException = getJobException(yarnAppId,jobId);
        if(StringUtils.isBlank(jobException)){
            return "";
        }
        JSONObject jobExceptionJSONObject = JSONObject.parseObject(jobException);
        if(jobExceptionJSONObject==null){
            return "";
        }
        String exception = jobExceptionJSONObject.getString("root-exception");
        if(StringUtils.isNotBlank(exception)){
            return exception;
        }else{
            return "";
        }
    }

    public static FlinkJobDTO getFlinkJobDtoWithRetry(String yarnAppId, String jobId) {
        Integer retryTimes = 2;
        if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
            return null;
        }
        FlinkJobDTO flinkJobDTO = null;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
            }
            try {
                String rs = FlinkWebClientBusiness.getJobDetail(yarnAppId,jobId);
                if(StringUtils.isNotBlank(rs)){
                    flinkJobDTO = JSONObject.parseObject(rs, FlinkJobDTO.class);
                    if(flinkJobDTO != null){
                        return flinkJobDTO;
                    }
                }
            } catch (Exception e) {
            }
            if (retryTimes-- <= 0) {
                return null;
            }
        }
    }

    /**
     * 等待job被取消，成功返回true，否则返回false，等待半分钟
     * @param yarnAppId
     * @param jobId
     * @return
     */
    public static boolean waitJobCanceledWithRetry(String yarnAppId, String jobId) {
        Integer retryTimes = 10;
        if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
            return true;
        }
        while (true) {
            try {
                FlinkJobDTO flinkJobDTO = FlinkWebClientBusiness.getFlinkJobDtoWithRetry(yarnAppId,jobId);
                if(flinkJobDTO == null){
                    //任务已经停止的时候， 直接返回true
                    return true;
                }
                LOGGER.error("waitJobCanceledWithRetry flinkJobDTO state = " + flinkJobDTO.getState());
                if(flinkJobDTO.getState().equals("CANCELED") || flinkJobDTO.getState().equals("FAILED")){
                    return true;
                }
            } catch (Exception e) {
            }
            if (retryTimes-- <= 0) {
                 return false;
            }
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
            }
        }
    }

    public static  List<FlinkVerticeMetricDto> getVerticeMetricsDtos(String yarnAppId, String jobId){
        List<FlinkVerticeMetricDto> flinkVerticeMetricDtos = Lists.newArrayList();

        if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
            return flinkVerticeMetricDtos;
        }

        List<FlinkJobVerticeDTO> flinkJobVerticeDTOs = getJobVerticeDTOs(yarnAppId,jobId);

        if(CollectionUtils.isEmpty(flinkJobVerticeDTOs)){
            return flinkVerticeMetricDtos;
        }

        for(FlinkJobVerticeDTO flinkJobVerticeDTO:flinkJobVerticeDTOs){

            String verticeMetrics = getVerticeMetrics( yarnAppId, jobId, flinkJobVerticeDTO.getId());
            if(StringUtils.isBlank(verticeMetrics)){
                continue;
            }

            List<FlinkMetricDto> flinkMetricDtos = Lists.newArrayList();

            FlinkVerticeMetricDto flinkVerticeMetricDto = new FlinkVerticeMetricDto();
            flinkVerticeMetricDto.setId(flinkJobVerticeDTO.getId());
            flinkVerticeMetricDto.setName(flinkJobVerticeDTO.getName());
            flinkVerticeMetricDto.setMetricDtos(flinkMetricDtos);
            flinkVerticeMetricDtos.add(flinkVerticeMetricDto);

            List<String> metrics = Lists.newArrayList();

            JSONArray jSONArray = JSONObject.parseArray(verticeMetrics);

            if(jSONArray!=null && jSONArray.size() > 0){
                for(int i=0;i < jSONArray.size();){
                    String id = jSONArray.getJSONObject(i).getString("id");

                    if(StringUtils.isNotBlank(id)){
                        try {
                            metrics.add(URLEncoder.encode(id,"UTF-8"));
                        } catch (UnsupportedEncodingException e) {
                        }
                    }

                    if(metrics.size() == 10){
                        List<FlinkMetricDto> rsflinkMetricDtos = getVerticeMetricsDtos(yarnAppId,jobId,flinkVerticeMetricDto.getId(), metrics);
                        if(CollectionUtils.isNotEmpty(rsflinkMetricDtos)){
                            flinkVerticeMetricDto.getMetricDtos().addAll(rsflinkMetricDtos);
                        }
                        metrics = Lists.newArrayList();
                    }

                    i++;
                }

                if(metrics.size()> 0){
                    List<FlinkMetricDto> rsflinkMetricDtos = getVerticeMetricsDtos(yarnAppId,jobId,flinkVerticeMetricDto.getId(), metrics);
                    if(CollectionUtils.isNotEmpty(rsflinkMetricDtos)){
                        flinkVerticeMetricDto.getMetricDtos().addAll(rsflinkMetricDtos);
                    }
                }
            }

            flinkVerticeMetricDto.setMetricDtos(flinkMetricDtos);
        }
        return flinkVerticeMetricDtos;
    }

    private static List<FlinkMetricDto> getVerticeMetricsDtos(String yarnAppId, String jobId, String verticeId, List<String> metrics){
        String address = buildYarnProxyAddress(yarnAppId) + "/jobs/" + jobId + "/vertices/" + verticeId + "/metrics";
        String rs = "";
        try {
            Map<String, Object> params = Maps.newHashMap();
            params.put("get",StringUtils.join(metrics,","));
            rs = instance.get(address,params);

            if(StringUtils.isNotBlank(rs) && isJsonArray(rs)){
                List<FlinkMetricDto> flinkMetricDtos = JSONObject.parseArray(rs,FlinkMetricDto.class);
                return flinkMetricDtos;
            }else if(StringUtils.isNotBlank(rs) && isJson(rs)){
                FlinkMetricDto flinkMetricDto = JSONObject.parseObject(rs,FlinkMetricDto.class);
                return Lists.<FlinkMetricDto>newArrayList(flinkMetricDto);
            }else{
                return Lists.<FlinkMetricDto>newArrayList();
            }
        } catch (IOException e) {
        }
        return Lists.<FlinkMetricDto>newArrayList();
    }

    private static String getVerticeMetrics(String yarnAppId, String jobId, String verticeId){
        String rs = "";
        try {
            if(StringUtils.isBlank(yarnAppId)){
                return "";
            }
            String address = buildYarnProxyAddress(yarnAppId) + "/jobs/" + jobId + "/vertices/" + verticeId + "/metrics";
            rs = instance.get(address,null);
            if(!isJsonArray(rs)){
                return "";
            }
        } catch (IOException e) {
        }
        return rs;
    }

    public static boolean yarnCancel(String yarnAppId, String jobId){
        if(StringUtils.isBlank(yarnAppId) || StringUtils.isBlank(jobId)){
            return false;
        }
        Integer retryTimes = 2;
        FlinkJobDTO flinkJobDTO = null;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
            }
            try {
                String address = buildYarnProxyAddress(yarnAppId) + "/jobs/" + jobId + "/yarn-cancel";
                if(instance.getReturnHttpCode(address,null) == 200){
                    return true;
                }
            } catch (Exception e) {
            }
            if (retryTimes-- <= 0) {
                return false;
            }
        }
    }
}
