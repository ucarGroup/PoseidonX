package com.ucar.streamsuite.yarn.registry;

import com.ucar.streamsuite.yarn.appmaster.JstormAMConstant;
import com.ucar.streamsuite.yarn.appmaster.JstormAMContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.registry.client.api.BindFlags;
import org.apache.hadoop.registry.client.api.RegistryOperations;
import org.apache.hadoop.registry.client.api.RegistryOperationsFactory;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.Endpoint;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.registry.server.integration.RMRegistryOperationsService;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerId;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static com.ucar.streamsuite.yarn.appmaster.JstormAMConstant.REGISTRY_ROOT;
import static org.apache.hadoop.registry.client.api.RegistryConstants.KEY_REGISTRY_ZK_ROOT;

/**
 * 将 jstormAM运行时的数据记录到zk
 */
public class RegistryClient {

    private static RegistryClient registryClient;

    //记录当前运行数据到zk,供其他地方使用
    private RegistryOperations registryOperations;

    public static RegistryClient init(String name,String serviceUserName,Configuration conf) throws IOException {
        registryClient = new RegistryClient();
        conf.set(KEY_REGISTRY_ZK_ROOT,REGISTRY_ROOT);
        registryClient.registryOperations = RegistryOperationsFactory.createInstance(name , conf);
        registryClient.setupInitialRegistryPaths(serviceUserName);
        registryClient.registryOperations.start();
        return registryClient;
    }

    private void setupInitialRegistryPaths(String serviceUserName) throws IOException {
        if (registryOperations instanceof RMRegistryOperationsService) {
            RMRegistryOperationsService rmRegOperations = (RMRegistryOperationsService) registryOperations;
            rmRegOperations.initUserRegistryAsync(serviceUserName);
        }
    }

    public static String getGlobalPath(){
        String hostPath = RegistryUtils.serviceclassPath(JstormAMConstant.REGISTYR_APP_TYPE,JstormAMConstant.REGISTYR_GLOBAL_HOST);
        return hostPath;
    }

    public static String getHostPath(String host){
        String hostPath = RegistryUtils.servicePath(JstormAMConstant.REGISTYR_APP_TYPE,JstormAMConstant.REGISTYR_GLOBAL_HOST,host);
        return hostPath;
    }

    public static String getApplicationPath(){
        String applicationPath = RegistryUtils.servicePath(JstormAMConstant.REGISTYR_APP_TYPE,JstormAMContext.clusterName,JstormAMContext.applicationId.toString());
        return applicationPath;
    }

    public static String getContainerPath(Container container){
        String applicationId = container.getId().getApplicationAttemptId().getApplicationId().toString();

        String containerId = container.getId().toString();
        String containerPath = RegistryUtils.componentPath(
                JstormAMConstant.REGISTYR_APP_TYPE, JstormAMContext.clusterName,applicationId,containerId);
        return containerPath;
    }
    public static String getContainerPath(ContainerId containerId){
        String applicationId = containerId.getApplicationAttemptId().getApplicationId().toString();

        String containerIdStr = containerId.toString();
        String containerPath = RegistryUtils.componentPath(
                JstormAMConstant.REGISTYR_APP_TYPE, JstormAMContext.clusterName,applicationId,containerIdStr);
        return containerPath;
    }

    public static ServiceRecord initApplicationServiceRecord(ServiceRecord oldApplication) {
        ServiceRecord application = oldApplication;
        if(application == null){
            application = new ServiceRecord();
        }

        application.set(YarnRegistryAttributes.YARN_ID, JstormAMContext.applicationId.toString());
        application.description = JstormAMConstant.REGISTRY_AM;
        application.set(YarnRegistryAttributes.YARN_PERSISTENCE, PersistencePolicies.PERMANENT);

        Map<String, String> addresses = new HashMap<String, String>();
        addresses.put(JstormAMConstant.REGISTRY_HOST, JstormAMContext.appMasterHostname);
        addresses.put(JstormAMConstant.REGISTRY_PORT, String.valueOf(JstormAMContext.appMasterHostPort));
        Endpoint endpoint = new Endpoint(JstormAMConstant.REGISTRY_HTTP, JstormAMConstant.REGISTRY_HOST_PORT, JstormAMConstant.REGISTRY_RPC, addresses);
        application.addExternalEndpoint(endpoint);
        return application;
    }

    public boolean mknode(String path) throws IOException {
        return registryOperations.mknode(path,true);
    }

    public void bind(String path, ServiceRecord serviceRecord) throws IOException {
         registryOperations.bind(path,serviceRecord,BindFlags.OVERWRITE);
    }

    public ServiceRecord resolve(String path) throws IOException {
        if (!registryOperations.exists(path)){
            return null;
        }
        return registryOperations.resolve(path);
    }

}
