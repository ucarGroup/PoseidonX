package com.ucar.streamsuite.engine.business;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.constant.JstormContainerTypeEnum;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.common.util.Base64Util;

import com.ucar.streamsuite.common.util.YarnClientProxy;
import com.ucar.streamsuite.common.util.ZKUtil;
import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.constants.YarnZkContant;
import com.ucar.streamsuite.engine.dto.AppContainerIdDTO;
import com.ucar.streamsuite.engine.dto.JstormYarnAppSubmitDTO;
import com.ucar.streamsuite.engine.service.impl.JstormEngineServiceImpl;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.assertj.core.util.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Description: 访问jstorm on yarn的业务类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class JStormOnYarnBusiness {

    public static final Logger LOGGER = LoggerFactory.getLogger(JStormOnYarnBusiness.class);

    /**
     * 创建jstorm on yarnApp
     */
    public static ApplicationId submitYarnApp(JstormYarnAppSubmitDTO jstormYarnAppSubmitDTO) throws Exception{

        List<String> jstormZkHosts= Lists.newArrayList(StringUtils.split(jstormYarnAppSubmitDTO.getJstormZkHost(),","));
        if(ZKUtil.pathExsit(jstormZkHosts,jstormYarnAppSubmitDTO.getJstormZkPort(),jstormYarnAppSubmitDTO.getJstormZkRoot())){
            throw new Exception("任务提交失败，存在同名的集群，请修改任务名称后重新提交任务！");
        }
        String clusterPath = YarnZkRegistryBusiness.PathBuilder.clusterPath(jstormYarnAppSubmitDTO.getClusterName());
        if(YarnZkRegistryBusiness.exists(clusterPath)){
            throw new Exception("任务提交失败，存在同名的集群，请修改任务名称后重新提交任务！");
        }

        YarnClientApplication app = YarnClientProxy.createApplication();
        if(app == null){
            throw new Exception("任务提交失败，无法创建 yarn App 请检查yarn集群环境是否正常！");
        }

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId appId = appContext.getApplicationId();
        if(appId == null){
            throw new Exception("任务提交失败，无法创建 yarn App 请检查yarn集群环境是否正常！");
        }

        //客户端类名
        appContext.setApplicationName(JstormEngineServiceImpl.class.getName());

        // 获得文件系统
        Configuration yarnCconfig = YarnClientProxy.getConf();
        FileSystem fs;
        try{
            fs = FileSystem.get(FileSystem.getDefaultUri(yarnCconfig), yarnCconfig, StreamContant.HADOOP_USER_NAME);
        }catch(InterruptedException e){
            LOGGER.error("submitYarnApp get hdfs filesystem is error",e);
            throw new Exception("任务提交失败，访问 hdfs 时发生异常，请检查yarn集群环境是否正常！");
        }

        // 设置localResources
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        Path appMaterJar =new Path(fs.getHomeDirectory(), jstormYarnAppSubmitDTO.getYarnAmJarPath());

        // 设置localResources -- appMaster
        FileStatus jarFIleStatus = fs.getFileStatus(appMaterJar);
        LocalResource scRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(appMaterJar.toUri()), LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION, jarFIleStatus.getLen(), jarFIleStatus.getModificationTime());
        // 替换掉前缀
        String yarnAmResouceName = jstormYarnAppSubmitDTO.getYarnAmJarPath().replace(StreamContant.HDFS_AM_PACKAGE_ROOT,"");
        localResources.put(yarnAmResouceName, scRsrc);

        // 设置localResources -- jstorm
        Path dstLocation =new Path(fs.getHomeDirectory(), jstormYarnAppSubmitDTO.getJstormJarPath());
        FileStatus scFileStatus = fs.getFileStatus(dstLocation);
        LocalResource scJstormRsrc = LocalResource.newInstance(ConverterUtils.getYarnUrlFromURI(dstLocation.toUri()), LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION, scFileStatus.getLen(), scFileStatus.getModificationTime());
        String jstormResouceName = "jstorm-2.2.1";
        localResources.put(jstormResouceName, scJstormRsrc);

        // 设置环境标量
        Map<String, String> env = new HashMap<String, String>();

        StringBuilder classPathEnv = new StringBuilder(ApplicationConstants.Environment.CLASSPATH.$$()).append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./*");
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR).append("./log_properties");

        env.put("APPMASTERJARSCRIPTLOCATION", appMaterJar.toUri().toString());
        env.put("APPMASTERLEN", Long.toString(jarFIleStatus.getLen()));
        env.put("APPMASTERTIMESTAMP", Long.toString(jarFIleStatus.getModificationTime()));
        env.put("CLASSPATH", classPathEnv.toString());

        //提交之前最后转换成网络地址提交给AM。
        String tempJstormJarPath = jstormYarnAppSubmitDTO.getJstormJarPath();
        jstormYarnAppSubmitDTO.setJstormJarPath(dstLocation.toUri().toString());

        //YarnSubmitter 必须是启动main方法的人。RM通过这个账号创建 appCache目录。Am也会根据此目录创建container目录
        String currentUser = StreamContant.HADOOP_USER_NAME;
        jstormYarnAppSubmitDTO.setYarnSubmitter(currentUser);

        String clusterStartParam = JSONObject.toJSONString(jstormYarnAppSubmitDTO);

        LOGGER.error("submitYarnApp jstormYarnAppSubmitDTO.toJSONString: "+clusterStartParam);

        String JSTORM_TASK_LOG_PREFIX = ConfigProperty.getConfigValue(ConfigKeyEnum.JSTORM_TASK_LOG_PREFIX);

        Vector<CharSequence> vargs = new Vector<CharSequence>(30);
        // Set java executable command
        vargs.add(ApplicationConstants.Environment.JAVA_HOME.$$() + "/bin/java");
        // Set Xmx based on am memory size
        vargs.add("-Xmx" + EngineContant.AM_DEFAULT_MEMORY + "m");
        // Set class name
        vargs.add(EngineContant.AM_CLASS);
        vargs.add(" ");
        vargs.add(Base64Util.encode(clusterStartParam));
        vargs.add("1>>" +  JSTORM_TASK_LOG_PREFIX + jstormYarnAppSubmitDTO.getClusterName() + "/Am_" + jstormYarnAppSubmitDTO.getClusterName() + ".out");
        vargs.add("2>>" +  JSTORM_TASK_LOG_PREFIX + jstormYarnAppSubmitDTO.getClusterName() + "/Am_" + jstormYarnAppSubmitDTO.getClusterName() + ".err");

        StringBuilder command = new StringBuilder();
        for (CharSequence str : vargs) {
            command.append(str).append(" ");
        }
        List<String> commands = new ArrayList<String>();
        commands.add("mkdir -p "+ JSTORM_TASK_LOG_PREFIX + jstormYarnAppSubmitDTO.getClusterName());
        commands.add( " && ");
        commands.add(command.toString());

        // 创建amContainer
        ContainerLaunchContext amContainer = ContainerLaunchContext.newInstance(localResources, env, commands, null, null, null);

        appContext.setResource(org.apache.hadoop.yarn.api.records.Resource.newInstance(EngineContant.AM_DEFAULT_MEMORY, EngineContant.AM_DEFAULT_VCORES));
        appContext.setAMContainerSpec(amContainer);
        appContext.setPriority(Priority.newInstance(EngineContant.AM_DEFAULT_PRIORITY));
        appContext.setQueue(EngineContant.AM_DEFAULT_QUEUE);
        appContext.setAttemptFailuresValidityInterval(5 * 1000);

        // 提交
        YarnClientProxy.submitApplication(appContext);
        YarnClientProxy.monitorAppSubmitComplate(appId.toString(),45);

        jstormYarnAppSubmitDTO.setJstormJarPath(tempJstormJarPath);
        return appId;
    }

    /**
     * 清除zk上的信息
     * @param clusterName
     * @param zkPort
     */
    public static void clearZkInfo(List<String> zkHosts,Integer zkPort,String clusterName) {
        if(CollectionUtils.isEmpty(zkHosts) || zkPort <= 0 || StringUtils.isBlank(clusterName)){
            return;
        }
        Integer retryTimes = 3;
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
            }
            try {
                //先删除jstorm集群zk
                String zkPath = EngineContant.YARN_APP_PREFIX + clusterName;

                boolean delereClusterIsOk = ZKUtil.delerePath(zkHosts,zkPort,zkPath);
                LOGGER.error("clearZkInfo delereJstormCluster is "+ delereClusterIsOk +" zkPath=" + zkPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort);

                String clusterPath = YarnZkRegistryBusiness.PathBuilder.clusterPath(clusterName);
                if(YarnZkRegistryBusiness.exists(clusterPath)){
                    boolean deleteYarnIsOk = YarnZkRegistryBusiness.delete(clusterPath);
                    LOGGER.error("clearZkInfo delereYarn is "+ deleteYarnIsOk +" zkPath=" + clusterPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort);
                    if(deleteYarnIsOk){
                        break;
                    }
                }
            } catch (Exception e) {
                LOGGER.error("clearZkInfo is error",e);
            }
            if (retryTimes-- <= 0) {
                break;
            }
        }
    }

    /**
     * 刷新并返回再用的appContainer信息
     */
    public static Set<AppContainerIdDTO> refreshAndGetZkAppContainer(String appId,String taskName) {
        Set<AppContainerIdDTO> allInUseZkContainers = Sets.newHashSet();
        try{
            Set<String> realAppContainerIds = Sets.newHashSet();
            //获得实际的应用的信息
            ApplicationReport applicationReport = YarnClientProxy.getApplicationReportByAppId(appId);
            if(applicationReport!=null){
                List<ContainerReport> containerReports = YarnClientProxy.getContainersByAppAttemptId(applicationReport.getCurrentApplicationAttemptId());
                if(CollectionUtils.isNotEmpty(containerReports)){
                    for(ContainerReport containerReport:containerReports){
                        realAppContainerIds.add(ConverterUtils.toString(containerReport.getContainerId()));
                    }
                }
            }

           //获得zk上的信息
            String componentListPath =  YarnZkRegistryBusiness.PathBuilder.componentListPath(taskName,appId);
            List<String> zkContainerIds = YarnZkRegistryBusiness.list(componentListPath);
            if(CollectionUtils.isNotEmpty(zkContainerIds)){
                Set<String> allInUseZkContainersStr = Sets.newHashSet();
                for(String zkContainerId: zkContainerIds){
                    try {
                        String containerPath = YarnZkRegistryBusiness.PathBuilder.containerPath(taskName,appId,zkContainerId);
                        ServiceRecord serviceRecord = YarnZkRegistryBusiness.resolve(containerPath);
                        if(serviceRecord == null){
                            continue;
                        }
                        // 如果没有再用则删掉
                        if(!realAppContainerIds.contains(zkContainerId)){
                            YarnZkRegistryBusiness.delete(containerPath);
                            continue;
                        }
                        allInUseZkContainersStr.add(zkContainerId);

                        AppContainerIdDTO appContainerIdDTO = new AppContainerIdDTO(taskName,appId,zkContainerId);
                        appContainerIdDTO.setServiceRecord(serviceRecord);
                        allInUseZkContainers.add(appContainerIdDTO);
                    } catch (Exception e) {
                        LOGGER.error("refreshAndGetAppContainer is error",e);
                    }
                }
            }
            return allInUseZkContainers;
        }catch (Exception e){
            LOGGER.error("refreshAndGetAppContainer is error",e);
            return allInUseZkContainers;
        }
    }

    /**
     * 刷新再用端口
     */
    public static void refreshInUsePortToZk(Set<AppContainerIdDTO> allInUseContainerIds) {
        //存每个主机，对应的WorkPorts
        Map<String,Set<String>> hostToSupervisorWorkPorts = new HashMap<String,Set<String>>();
        Set<String> nimbusPorts = new HashSet<String>();
        Set<String> logViewPorts = new HashSet<String>();

        for(AppContainerIdDTO inUseContainerId:allInUseContainerIds){
            try {
                ServiceRecord serviceRecord = inUseContainerId.getServiceRecord();
                if(serviceRecord == null){
                    continue;
                }
                //跳过停止的
                String containerStatus = serviceRecord.get(YarnZkContant.ZK_CONTAINER_STATUS);
                if(StringUtils.isNotBlank(containerStatus) && containerStatus.equals(YarnZkContant.ZK_CONTAINER_STOP)){
                    continue;
                }
                // 如果是 nimbus 得到 container_port。和 jstorm_log_port
                if(inUseContainerId.getContainerType() == JstormContainerTypeEnum.NIMBUS){
                    String jstormLogPort = serviceRecord.get(YarnZkContant.ZK_CONTAINER_LOG_PORT);
                    if(StringUtils.isNotBlank(jstormLogPort)){
                        logViewPorts.add(jstormLogPort);
                    }
                    String jstormNimbusPort = serviceRecord.get(YarnZkContant.ZK_CONTAINER_NIMBUS_PORT);
                    if(StringUtils.isNotBlank(jstormNimbusPort)){
                        nimbusPorts.add(jstormNimbusPort);
                    }
                }else{
                    // 如果是 supervisor 得到 supervisor_port_list 和 jstorm_log_port
                    String jstormLogPort = serviceRecord.get(YarnZkContant.ZK_CONTAINER_LOG_PORT);
                    if(StringUtils.isNotBlank(jstormLogPort)){
                        logViewPorts.add(jstormLogPort);
                    }
                    String supervisorPortList = serviceRecord.get(YarnZkContant.ZK_CONTAINER_SUPERVISOR_PORT_LIST);
                    String jstormHost = serviceRecord.get(YarnZkContant.ZK_CONTAINER_JSTORM_HOST);

                    if(!hostToSupervisorWorkPorts.containsKey(jstormHost)){
                        hostToSupervisorWorkPorts.put(jstormHost,Sets.<String>newHashSet());
                    }

                    if(StringUtils.isNotBlank(supervisorPortList) && !supervisorPortList.equals("0") && StringUtils.isNotBlank(jstormHost)){
                        for(String port:StringUtils.split(supervisorPortList,",")){
                            hostToSupervisorWorkPorts.get(jstormHost).add(port);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("refreshInUsePort is error", e);
            }
        }

        // 刷新hostNode的 supervisor_port_list
        String globalPath = YarnZkRegistryBusiness.PathBuilder.globalPath();
        List<String> hostNodes;
        Set<String> allConfigPorts = new HashSet<String>();
        try {
            hostNodes = YarnZkRegistryBusiness.list(globalPath);
            //处理每一个主机
            if(CollectionUtils.isNotEmpty(hostNodes)){
                for(String hostNode: hostNodes){
                    try {
                        String hostPath = YarnZkRegistryBusiness.PathBuilder.hostPath(hostNode);
                        ServiceRecord serviceRecord = YarnZkRegistryBusiness.resolve(hostPath);
                        if(serviceRecord == null){
                            continue;
                        }

                        //拿到配置的冲突端口
                        Set<String> configPorts = new HashSet<String>();
                        if(serviceRecord.get(YarnZkContant.ZK_HOST_CONFLICT_PORT) == null){
                            serviceRecord.set(YarnZkContant.ZK_HOST_CONFLICT_PORT,"");
                        }else{
                            String conflict_port = serviceRecord.get(YarnZkContant.ZK_HOST_CONFLICT_PORT);
                            if(StringUtils.isNotBlank(conflict_port)){
                                configPorts = Sets.newHashSet(StringUtils.split(conflict_port,","));
                            }
                        }

                        allConfigPorts.addAll(configPorts);

                        Set<String> supervisorWorkPorts =  hostToSupervisorWorkPorts.get(hostNode);
                        if(supervisorWorkPorts == null){
                            supervisorWorkPorts = configPorts;
                        }else{
                            supervisorWorkPorts.addAll(configPorts);
                        }

                        serviceRecord.set(YarnZkContant.ZK_CONTAINER_SUPERVISOR_PORT_LIST,StringUtils.join(supervisorWorkPorts,","));
                        YarnZkRegistryBusiness.bind(hostPath,serviceRecord);
                    } catch (Exception e) {
                        LOGGER.error("refreshInUsePort is error host =" + hostNode + " supervisorWorkPorts=" + StringUtils.join(hostToSupervisorWorkPorts.get(hostNode),","), e);
                    }
                }
            }

            // 写globalPath的 nimbusPorts和logViewPorts
            ServiceRecord serviceRecord = YarnZkRegistryBusiness.resolve(globalPath);
            nimbusPorts.addAll(allConfigPorts);
            logViewPorts.addAll(allConfigPorts);
            if(serviceRecord!=null){
                serviceRecord.set(YarnZkContant.ZK_GLOBAL_LOGVIEW,StringUtils.join(logViewPorts,","));
                serviceRecord.set(YarnZkContant.ZK_GLOBAL_NIMBUS_PORT,StringUtils.join(nimbusPorts,","));
                YarnZkRegistryBusiness.bind(globalPath,serviceRecord);
            }
        } catch (Exception e) {
            LOGGER.error("refreshInUsePort is error",e);
        }
    }

    /**
     * 删除应用
     * @param zkHosts
     * @param zkPort
     * @param appId
     */
    public static void killApplication(List<String> zkHosts, Integer zkPort, String appId, String taskName) throws Exception{
        if(StringUtils.isNotBlank(appId) && StringUtils.isNotBlank(taskName)){

            String applicationPath = YarnZkRegistryBusiness.PathBuilder.applicationPath(taskName,appId);
            ServiceRecord serviceRecord = YarnZkRegistryBusiness.resolve(applicationPath);
            if(serviceRecord !=null){
                serviceRecord.set("killed","true");
                YarnZkRegistryBusiness.bind(applicationPath,serviceRecord);
            }
            ApplicationReport applicationReport = YarnClientProxy.getApplicationReportByAppId(appId);
            if(applicationReport !=null){
                try{
                    YarnClientProxy.killApplicationByAppId(appId);
                }catch (Exception e){
                    if(serviceRecord != null){
                        serviceRecord.set("killed","false");
                        YarnZkRegistryBusiness.bind(applicationPath,serviceRecord);
                    }
                    throw e;
                }
            }
        }
        JStormOnYarnBusiness.clearZkInfo(zkHosts,zkPort,taskName);
    }
}
