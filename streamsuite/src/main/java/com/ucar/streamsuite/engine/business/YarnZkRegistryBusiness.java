package com.ucar.streamsuite.engine.business;

import com.ucar.streamsuite.common.constant.StreamContant;

import com.ucar.streamsuite.engine.constants.YarnZkContant;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryConstants;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;

@Component
public class YarnZkRegistryBusiness {

    private static final Log LOG = LogFactory.getLog(YarnZkRegistryBusiness.class);

    //记录当前运行数据到zk,供其他地方使用
    private static RegistryOperations registryOperations = null;

    //初始化静态参数
    @PostConstruct
    public void init() {
        initAndStart();
    }

    private static void initAndStart(){
        try{
            Configuration conf  = new YarnConfiguration();
            conf.set(RegistryConstants.KEY_REGISTRY_ZK_ROOT, YarnZkContant.ZK_AM_REGISTRY_ROOT);
            registryOperations = RegistryOperationsFactory.createInstance(YarnZkContant.ZK_AM_REGISTRY_JSTORM_YARN, conf);
            if (registryOperations instanceof RMRegistryOperationsService) {
                RMRegistryOperationsService rmRegOperations = (RMRegistryOperationsService) registryOperations;
                rmRegOperations.initUserRegistryAsync(StreamContant.HADOOP_USER_NAME);
            }
        }catch(Exception e){
        }
        if(registryOperations==null){
            throw new IllegalArgumentException("YarnZkClientBusiness registryOperations start is error，cann't connect yarn zk！！");
        }
        registryOperations.start();
    }

    public static boolean exists(String path) throws IOException {
        return registryOperations.exists(path);
    }

    public static boolean bind(String path, ServiceRecord serviceRecord){
        try{
            registryOperations.bind(path,serviceRecord,BindFlags.OVERWRITE);
            return true;
        }catch(Exception e){
            return false;
        }
    }

    public static ServiceRecord resolve(String path) {
        try{
            if (!registryOperations.exists(path)){
                return null;
            }
            return registryOperations.resolve(path);
        }catch(Exception e){
            return null;
        }
    }

    public static boolean delete(String path)  {
        try{
            registryOperations.delete(path,true);
            return true;
        }catch(Exception e){
            return false;
        }
    }

    public static List<String> list(String path)  {
        try{
            return registryOperations.list(path);
        }catch(Exception e){
            return null;
        }
    }

    public static class PathBuilder {
        public static String globalPath(){
            String globalPath = RegistryUtils.serviceclassPath(YarnZkContant.ZK_AM_REGISTYR_APP_TYPE,YarnZkContant.ZK_AM_REGISTYR_GLOBAL_HOST);
            return globalPath;
        }

        public static String hostPath(String host){
            String hostPath = RegistryUtils.servicePath(YarnZkContant.ZK_AM_REGISTYR_APP_TYPE,YarnZkContant.ZK_AM_REGISTYR_GLOBAL_HOST,host);
            return hostPath;
        }

        public static String clusterPath(String clusterName){
            String clusterPath = RegistryUtils.serviceclassPath(YarnZkContant.ZK_AM_REGISTYR_APP_TYPE,clusterName);
            return clusterPath;
        }

        public static String applicationPath(String clusterName, String applicationId){
            String applicationPath = RegistryUtils.servicePath(YarnZkContant.ZK_AM_REGISTYR_APP_TYPE,clusterName,applicationId);
            return applicationPath;
        }

        public static String containerPath(String clusterName, String applicationId, String containerId){
            String containerPath = RegistryUtils.componentPath(YarnZkContant.ZK_AM_REGISTYR_APP_TYPE, clusterName,applicationId,containerId);
            return containerPath;
        }

        public static String componentListPath(String clusterName, String applicationId){
            String containerPath = RegistryUtils.componentListPath(YarnZkContant.ZK_AM_REGISTYR_APP_TYPE, clusterName,applicationId);
            return containerPath;
        }
    }
}
