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
package com.ucar.streamsuite.yarn.appmaster;

import com.ucar.streamsuite.yarn.exception.JstormAMException;
import com.ucar.streamsuite.yarn.registry.RegistryClient;
import com.ucar.streamsuite.yarn.registry.SlotPortsView;
import com.ucar.streamsuite.yarn.utils.Base64Util;
import com.ucar.streamsuite.yarn.utils.JstormAMUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.registry.client.binding.RegistryUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.registry.client.types.yarn.PersistencePolicies;
import org.apache.hadoop.registry.client.types.yarn.YarnRegistryAttributes;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.ExitUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static com.ucar.streamsuite.yarn.appmaster.JstormAMContext.*;

/**
 * jstorm 的 application master
 * 也就是jstormAM
 * 区别于官方原版
 */
@SuppressWarnings({"unchecked"})
public class JstormMaster {

    private static final Log LOG = LogFactory.getLog(JstormMaster.class);

    //hadoop的用户信息
    private UserGroupInformation appSubmitterUgi;

    // AMclient处理和RM的连接
    private AMRMClientAsync amRMClient;

    //NM的连接类
    private NMClientAsync nmClientAsync;

    // 监听NM返回结果类
    private NMCallbackHandler containerListener;

    //用来保存启动container的命令的线程句柄
    private List<Thread> launchThreads = new ArrayList<Thread>();

    //用来记录当前运行信息到zk
    private RegistryClient registryClient;

    //记录端口
    private SlotPortsView slotPortsView;

    /**
     * jstormAM的入口函数
     */
    public static void main(String[] args) {
        boolean result = false;
        try {
            if (null == args || args.length == 0) {
                LOG.error("### JstormAM运行错误,传入的args为空!");
                return;
            }
            //构建jstormAM实例
            JstormMaster appMaster = new JstormMaster();
            //解析client的参数
            appMaster.dealJstormAppPrarm(args[0]);
            //启动jstormAM
            appMaster.run();
            //进行jstormAM退出时候的扫尾工作
            result = appMaster.finish();
        } catch (JstormAMException e) {
            LOG.fatal("### jstormAM 运行异常[JstormAMException]", e);
            ExitUtil.terminate(JstormAMConstant.EXIT_FAIL1, e);
        }
        catch (Throwable t) {
            LOG.fatal("### Error running JstormMaster[Throwable]", t);
            ExitUtil.terminate(JstormAMConstant.EXIT_FAIL1, t);
        }
        if (result) {
            LOG.info("### Applicataion Master completed successfully. exiting");
            System.exit(JstormAMConstant.EXIT_SUCCESS);
        } else {
            LOG.info("### Application Master failed. exiting");
            System.exit(JstormAMConstant.EXIT_FAIL2);
        }
    }

    /**
     * JstormAM 的构造函数
     */
    public JstormMaster() throws IOException {
        conf = new YarnConfiguration();
        Path jstormYarnConfPath = new Path("jstorm-yarn.xml");
        conf.addResource(jstormYarnConfPath);

        JstormAMContext.serviceUserName = RegistryUtils.currentUser();
        LOG.error("### JstormAMContext.serviceUserName:["+JstormAMContext.serviceUserName+"]");
        registryClient = RegistryClient.init(JstormAMConstant.REGISTRY_JSTORM_YARN,JstormAMContext.serviceUserName,conf);
        slotPortsView = new SlotPortsView(registryClient);
        slotPortsView.setMinPort(JstormAMConstant.SUPERVISOR_MIN_PORT);
        slotPortsView.setMaxPort(JstormAMConstant.SUPERVISOR_MAX_PORT);
        JstormAMContext.requestBlockingQueue = new LinkedBlockingQueue<ContainerRequest>();

        JstormAMContext.currentNimbusNum = new AtomicInteger(0);
        JstormAMContext.currentSupervisorNum = new AtomicInteger(0);
    }

    /***
     * 处理 client启动am的时候传过来的参数
     * 这个参数是使用base64编码的json字符串
     * @param param
     */
    private void dealJstormAppPrarm(String param) throws Exception {

        String yarnParamVOString=param;

        //将client传过来的参数 进行 base64解码,得到一个jstorm格式的字符串
        yarnParamVOString =Base64Util.decode(yarnParamVOString);
        LOG.error("### 启动AM的参数:["+yarnParamVOString+"]");

        //将json字符串解析为 pojo 对象
        JstormYarnAppSubmitDTO yarnParamVO =  com.alibaba.fastjson.JSONObject.parseObject(yarnParamVOString, JstormYarnAppSubmitDTO.class);

        //处理nimbus的数量
        if(yarnParamVO.getNimbusNum()!= null){
            nimbusNum=yarnParamVO.getNimbusNum();
        }else{
            throw new JstormAMException("### 参数nimbusNum为空!");
        }
        //处理nimbus的内存
        if(yarnParamVO.getNimbusMemery()!= null){
            nimbusMemory =yarnParamVO.getNimbusMemery();
        }else{
            throw new JstormAMException("### 参数nimbusMemery为空!");
        }
        //处理supervisor的数量
        if(yarnParamVO.getSupervisorNum()!=null){
            supervisorNum=yarnParamVO.getSupervisorNum();
        }else{
            throw new JstormAMException("### 参数supervisorNum为空!");
        }
        //处理supervisor的内存
        if(yarnParamVO.getSupervisorMemery()!=null){
            supervisorMemory = yarnParamVO.getSupervisorMemery();
        }else{
            throw new JstormAMException("### 参数supervisorMemory为空!");
        }
        jstormJarPath=yarnParamVO.getJstormJarPath();
        appMasterJarPath=yarnParamVO.getYarnAmJarPath();
        zkAddress=yarnParamVO.getJstormZkHost();
        zkPort=yarnParamVO.getJstormZkPort().toString();
        zkRoot=yarnParamVO.getJstormZkRoot();
        appId=yarnParamVO.getYarnAppId();
        userName=yarnParamVO.getYarnSubmitter();
        clusterName=yarnParamVO.getClusterName();
        supervisorPortNum=yarnParamVO.getSupervisorportNum();

        //计算本应用申请的最大内存
        JstormAMContext.applyMemoryForNimbusContainer = nimbusMemory;
        JstormAMContext.applyMemoryForSupervisorContainer = supervisorMemory;
        //计算本应用申请的最大cpu
        JstormAMContext.applyVcores = 1;

        String containerIdStr = System.getenv(ApplicationConstants.Environment.CONTAINER_ID.name());
        LOG.error("### AM的Container的id为:["+containerIdStr+"]");

        JstormAMContext.containerForAMId = ConverterUtils.toContainerId(containerIdStr);
        JstormAMContext.applicationAttemptId = containerForAMId.getApplicationAttemptId();
        JstormAMContext.applicationId = JstormAMContext.applicationAttemptId.getApplicationId();

        //设置这个应用的nimbus的端口,这个是全局唯一的
        JstormAMContext.nimbusPort = slotPortsView.getAvailablePort(JstormAMConstant.REGISTRY_TYPE_NIMBUS);

        //设置这个应用的logview端口,这个是全局唯一的
        JstormAMContext.logviewPort = slotPortsView.getAvailablePort(JstormAMConstant.REGISTRY_TYPE_LOGVIEW);
    }

    /**
     * AM运行的主要函数
     *
     * @throws YarnException
     * @throws IOException
     */

    public void run() throws Exception {
        LOG.error("##### Starting JstormMaster");
        JstormAMContext.isRunning = true;

        /*************
         *  用户操作yarn集群的权限相关
         */
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        DataOutputBuffer dob = new DataOutputBuffer();
        credentials.writeTokenStorageToStream(dob);
        Iterator<Token<?>> iter = credentials.getAllTokens().iterator();
        LOG.info("### Executing with tokens:");
        while (iter.hasNext()) {
            Token<?> token = iter.next();
            LOG.info(token);
            if (token.getKind().equals(AMRMTokenIdentifier.KIND_NAME)) {
                iter.remove();
            }
        }
        allTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
        appSubmitterUgi = UserGroupInformation.createRemoteUser(userName);
        appSubmitterUgi.addCredentials(credentials);

        /*************
         *  AM->RM client 相关
         */
        LOG.error("##### Starting AMRMClientAsync");
        AMRMClientAsync.CallbackHandler allocListener = new RMCallbackHandler();
        amRMClient = AMRMClientAsync.createAMRMClientAsync(JstormAMConstant.AM_RM_CLIENT_INTERVAL, allocListener);
        amRMClient.init(conf);
        amRMClient.start();

        /*************
         *  AM->NM client 相关
         */
        LOG.error("##### Starting nmClientAsync");
        containerListener = createNMCallbackHandler();
        nmClientAsync = new NMClientAsyncImpl(containerListener);
        nmClientAsync.init(conf);
        nmClientAsync.start();

        //获取当前am所处机器的ip,并且到RM上进行注册，保持心跳连接
        JstormAMContext.appMasterHostname = NetUtils.getHostname();

        //获取可用端口作为am使用的端口
        JstormAMContext.appMasterHostPort = JstormAMUtil.getAvailablePort();

        //将这个jstormAM注册到rm上
        RegisterApplicationMasterResponse response = amRMClient.registerApplicationMaster(JstormAMContext.appMasterHostname, JstormAMContext.appMasterHostPort,JstormAMContext.appMasterTrackingUrl);

        // 获取目前yarn集群的最大可用内存
        JstormAMContext.maxMemory = response.getMaximumResourceCapability().getMemory();
        LOG.error("##### 当前YARN集群最大可用内存: " + JstormAMContext.maxMemory);

        // 获取目前yarn集群的最大可用cpu
        JstormAMContext.maxVcores = response.getMaximumResourceCapability().getVirtualCores();
        LOG.error("##### 当前YARN集群最大可用cpu: " + JstormAMContext.maxVcores);


        /*********
         *  如果AM是重启的,则会 读取 之前的container信息
         */
        List<Container> previousAMRunningContainers = response.getContainersFromPreviousAttempts();
        LOG.error("### application["+amRMClient.getName()+"],appId:["+appId+"]: "+previousAMRunningContainers.size());

        //遍历之前的container信息
        for (Container container : previousAMRunningContainers) {

            String applicationAttemptId = container.getId().getApplicationAttemptId().toString();
            Long containerId = container.getId().getContainerId();

            String containerPath = registryClient.getContainerPath(container);
            ServiceRecord serviceRecord = registryClient.resolve(containerPath);

            LOG.error("##### 上一次的信息:applicationAttemptId:["+applicationAttemptId+"]" + ",containerId["+containerId+"]" + ",containerPath:["+containerPath+"]" + ",serviceRecord:["+serviceRecord.toString()+"]");
        }

        /***************
         * 记录应用的信息
         */
        String applicationPath = registryClient.getApplicationPath();

        LOG.error("记录application信息 PATH:["+applicationPath+"]");
        ServiceRecord preApplicationServiceRecord = registryClient.resolve(applicationPath);
        ServiceRecord applicationServiceRecord = RegistryClient.initApplicationServiceRecord(preApplicationServiceRecord);
        registryClient.mknode(applicationPath);
        registryClient.bind(applicationPath, applicationServiceRecord);

        /*************
         * 发出对于container的申请
         */
        //如果当前的AM是运行状态
        if (JstormAMContext.isRunning) {

            //申请nimbus的container
            for (int i = 0; i < nimbusNum; ++i) {
                ContainerRequest containerAsk = JstormAMUtil.setupContainerAskForRM(JstormAMConstant.CONTAINER_TYPE_NIMBUS, nimbusMemory);
                amRMClient.addContainerRequest(containerAsk);
            }

            //申请supervisor的container
            for (int i = 0; i < supervisorNum; ++i) {
                ContainerRequest containerAsk = JstormAMUtil.setupContainerAskForRM(JstormAMConstant.CONTAINER_TYPE_SUPERVISOR,supervisorMemory);
                amRMClient.addContainerRequest(containerAsk);
            }
        }
    }

    /**
     * 创建NMCallbackHandler的监听函数
     * @return
     */
    private NMCallbackHandler createNMCallbackHandler() {
        return new NMCallbackHandler(this);
    }

    /**
     * AM<->RM的监听函数
     */
    private class RMCallbackHandler implements AMRMClientAsync.CallbackHandler {

        /***
         * container 运行完毕后进入的回调函数
         * @param completedContainers
         */

        @Override
        public void onContainersCompleted(List<ContainerStatus> completedContainers) {
            //conatiner运行完毕后，进行处理
            LOG.error("### onContainersCompleted:[" + completedContainers.size() + "]");

            for (ContainerStatus containerStatus : completedContainers) {

                LOG.error("### ContainerId:["+containerStatus.getContainerId().toString()+"]," +
                             "ExitStatus:["+containerStatus.getExitStatus()+"]," +
                             "containerStatus-state:[" + containerStatus.getState().name() + "]" +
                             "Diagnostics:["+containerStatus.getDiagnostics()+"]");

                ServiceRecord containerServiceRecord = null;
                try {
                    //将这个container状态设置为停止
                    ContainerId containerId = containerStatus.getContainerId();
                    String containerPath = registryClient.getContainerPath(containerId);
                    containerServiceRecord = registryClient.resolve(containerPath);

                    //如果 container 为空,说明这个container是多余申请的,并没有被真正使用
                    if(containerServiceRecord == null){
                        LOG.error("### Containers["+containerStatus.getContainerId().toString()+"] 是被多余申请的,不应该对其做处理,直接跳过.");
                        continue;
                    }

                    containerServiceRecord.set(JstormAMConstant.REGISTRY_STATUS,JstormAMConstant.REGISTRY_STATUS_STOPED);
                    registryClient.bind(containerPath,containerServiceRecord);

                    LOG.error("### onContainersCompleted ["+containerStatus.getContainerId().toString()+"] containerServiceRecord:["+containerServiceRecord.toString()+"]");
                } catch (Exception e) {
                    LOG.error("### onContainersCompleted ["+containerStatus.getContainerId().toString()+"] save containerServiceRecord error",e);
                }

                //获取对应的applicaton的信息
                String path = registryClient.getApplicationPath();
                try {
                    ServiceRecord applicationServiceRecord = registryClient.resolve(path);
                    if(applicationServiceRecord != null){
                        String killed = applicationServiceRecord.get(JstormAMConstant.REGISTRY_KILLED);

                        LOG.error("### ContainerId:["+containerStatus.getContainerId().toString()+"]状态,killed["+killed+"]");

                        //当killed 属性不存在或者不为true时,表示不是正常退出
                        //此时要重启container
                        if(killed == null || !"true".equals(killed)){
                            //获取container的参数,申请新的container
                            String jstormType = containerServiceRecord.get(JstormAMConstant.REGISTRY_JSTORM_TYPE);
                            ContainerRequest containerAsk;

                            //nimbus
                            if(JstormAMConstant.REGISTRY_TYPE_NIMBUS.equals(jstormType)){
                                 containerAsk = JstormAMUtil.setupContainerAskForRM(JstormAMConstant.CONTAINER_TYPE_NIMBUS, nimbusMemory);
                                 JstormAMContext.currentNimbusNum.decrementAndGet();
                            }
                            //supervisor
                            else{
                                 containerAsk = JstormAMUtil.setupContainerAskForRM(JstormAMConstant.CONTAINER_TYPE_SUPERVISOR,supervisorMemory);
                                JstormAMContext.currentSupervisorNum.decrementAndGet();
                            }

                            amRMClient.addContainerRequest(containerAsk);
                            try {
                                 JstormAMContext.requestBlockingQueue.add(containerAsk);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    }
                } catch (Exception e) {
                    LOG.error("### onContainersCompleted["+containerStatus.getContainerId().toString()+"] error",e);
                }
            }
        }

        /**
         * container 分配成功后进入的回调函数
         * @param allocatedContainers
         */
        @Override
        public void onContainersAllocated(List<Container> allocatedContainers) {
            try{
                LOG.error("### container申请成功的个数:["+allocatedContainers.size()+"]");

            for (Container allocatedContainer : allocatedContainers) {

                ContainerId containerId = allocatedContainer.getId();
                int containerType = allocatedContainer.getPriority().getPriority();
                String allocatedContainerHost = allocatedContainer.getNodeId().getHost();
                int allocatedContainerPort = allocatedContainer.getNodeId().getPort();
                String allocatedContainerHttpAddress = allocatedContainer.getNodeHttpAddress();
                int allocatedContainerMemory = allocatedContainer.getResource().getMemory();
                int allocatedContainerVirtualCores =  allocatedContainer.getResource().getVirtualCores();
                String containerPath = registryClient.getContainerPath(allocatedContainer);

                LOG.error("#### 成功申请的container明细:"
                        + "containerId=" + containerId
                        + ", allocatedContainerHost=" + allocatedContainerHost+ ":" + allocatedContainerPort
                        + ", allocatedContainerHttpAddress=" + allocatedContainerHttpAddress
                        + ", containerResourceMemory="+ allocatedContainerMemory
                        + ", containerResourceVirtualCores="+ allocatedContainerVirtualCores
                        + ", containerType="+containerType
                        + ", containerPath="+containerPath);

                // need to remove container request when allocated,
                // otherwise RM will continues allocate container over needs
                if (!JstormAMContext.requestBlockingQueue.isEmpty()) {
                    try {
                        LOG.error("#### 移除申请container");
                        amRMClient.removeContainerRequest(JstormAMContext.requestBlockingQueue.take());
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                //container的数目已经超出申请,不能使用
                if(!compareContainerSize(containerType)){
                    amRMClient.releaseAssignedContainer(containerId);
                    continue;
                }

                /*******
                 * 启动对应的container
                 */
                LaunchContainerRunnable runnableLaunchContainer = new LaunchContainerRunnable(allocatedContainer, containerListener, containerType);
                Thread launchThread = new Thread(runnableLaunchContainer);
                launchThreads.add(launchThread);
                launchThread.start();

            }}catch (Exception e) {
                LOG.error(e);
            }
        }

        @Override
        public void onShutdownRequest() {
            LOG.error("##### onShutdownRequest");

            //JstormAMContext.isRunning = false;
        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {

            LOG.error("##### onNodesUpdated");
        }

        @Override
        public float getProgress() {
            return 0.5f;
        }

        @Override
        public void onError(Throwable e) {
            LOG.error("##### onError",e);

            JstormAMContext.isRunning = false;
            amRMClient.stop();
        }
    }

    /***
     *  AM-NM监听器
     */
    private static class NMCallbackHandler implements NMClientAsync.CallbackHandler {

        private ConcurrentMap<ContainerId, Container> containers = new ConcurrentHashMap<ContainerId, Container>();
        private final JstormMaster applicationMaster;

        public NMCallbackHandler(JstormMaster applicationMaster) {
            this.applicationMaster = applicationMaster;
        }

        public void addContainer(ContainerId containerId, Container container) {
            LOG.error("##### addContainer:"+containerId);

            containers.putIfAbsent(containerId, container);
        }

        @Override
        public void onContainerStopped(ContainerId containerId) {
            LOG.error(" #### ----onContainerStopped:"+containerId );

            containers.remove(containerId);
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId,ContainerStatus containerStatus) {
            LOG.error("#### ----onContainerStatusReceived:id=" + containerId + ", status="+containerStatus );

        }

        @Override
        public void onContainerStarted(ContainerId containerId,Map<String, ByteBuffer> allServiceResponse) {
            LOG.error("#### ----onContainerStarted["+containerId+"]" );

            Container container = containers.get(containerId);
            if (container != null) {
                applicationMaster.nmClientAsync.getContainerStatusAsync(containerId, container.getNodeId());
            }
        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {
            LOG.error("#### Failed to start Container:["+containerId+"]");

            containers.remove(containerId);
        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {
            LOG.error("##### onGetContainerStatusError:["+containerId+"]",t);
        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {
            LOG.error("#### onStopContainerError:["+containerId+"]",t);

            containers.remove(containerId);
        }
    }

    /****
     * 执行 启动具体container的命令的线程
     */
    private class LaunchContainerRunnable implements Runnable {

        Container container;
        NMCallbackHandler containerListener;
        int containerType;

        /**
         * @param lcontainer        Allocated container
         * @param containerListener Callback handler of the container
         */
        public LaunchContainerRunnable(
                Container lcontainer, NMCallbackHandler containerListener, int containerType) {
            this.container = lcontainer;
            this.containerListener = containerListener;
            this.containerType = containerType;
        }

        @Override
        public void run() {
            LOG.error("#### LaunchContainerRunnable run start containerid="+ container.getId());
            /******
             * 设置 jstorm集群的文件的路径
             */
            Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
            try{
                FileSystem fileSystem = FileSystem.get(conf);
                LocalResource jstormzip = Records.newRecord(LocalResource.class);
                jstormzip.setType(LocalResourceType.ARCHIVE);
                jstormzip.setVisibility(LocalResourceVisibility.APPLICATION);
                Path jstormzipConfPath = new Path(new URI(jstormJarPath));
                jstormzip.setResource(ConverterUtils.getYarnUrlFromPath(jstormzipConfPath));
                jstormzip.setTimestamp(fileSystem.getFileStatus(jstormzipConfPath).getModificationTime());
                jstormzip.setSize(fileSystem.getFileStatus(jstormzipConfPath).getLen());
                localResources.put(JstormAMConstant.JSTORM_SOFTLINK, jstormzip);
            }catch (Exception e) {
                LOG.error(" 获取 hdfs filestatus 异常, path=" + jstormJarPath, e);
            }

            //拼装命令行的list
            List<String> commands = new ArrayList<String>();
            commands.add("mkdir -p "+JstormAMConstant.LOG_PATH+JstormAMContext.clusterName);
            commands.add( " && ");

            try {
                String host = this.container.getNodeId().getHost();
                int containerPort = container.getNodeId().getPort();
                String yarnId = container.getId().toString();

                //设置logview的端口
                long logviewPort = JstormAMContext.logviewPort;
                //获取nimbus端口
                long nimbusPort = JstormAMContext.nimbusPort;

                String containerType = JstormAMUtil.getContainerType(this.containerType);

                String supervisorPortStr = "0";
                if(containerType.equals(JstormAMConstant.REGISTRY_TYPE_SUPERVISOR)) {
                    //获取supervisor端口列表
                    List<String> supervisorPortList = slotPortsView.getAvailableSupervisorList(host, JstormAMContext.supervisorPortNum);
                    if(supervisorPortStr != null && supervisorPortStr.length()>0) {
                        supervisorPortStr = org.apache.hadoop.util.StringUtils.arrayToString(supervisorPortList.toArray(new String[0]));
                    }
                }

                String containerHomePath = JstormAMUtil.getContainerHome(container,userName);
                String jstormHomePath=containerHomePath+"/"+JstormAMConstant.JSTORM_SOFTLINK;

                String jstormStartComond="sh "+ jstormHomePath +"/start_jstorm.sh "
                        + containerType +" "
                        + jstormHomePath +" "
                        + logviewPort +" "
                        + nimbusPort +" "
                        + containerHomePath +" "
                        + zkRoot +" "
                        + zkAddress +" "
                        + zkPort  +" "
                        + supervisorPortStr +" "
                        + JstormAMContext.clusterName
                        + " 1>>"+JstormAMConstant.LOG_PATH+JstormAMContext.clusterName+"/"+containerType+"_"+clusterName+"_"+containerPort+".log"
                        + " 2>>"+JstormAMConstant.LOG_PATH+JstormAMContext.clusterName+"/"+containerType+"_"+clusterName+"_"+containerPort+".err";

                LOG.error("#### 执行的命令 jstormStartComond : [" + jstormStartComond+"]");

                commands.add(jstormStartComond.toString());

                //记录信息
                ServiceRecord serviceRecord = new ServiceRecord();
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_HOST,host);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_NIMBUS_PORT,nimbusPort);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_LOG_PORT,logviewPort);
                serviceRecord.set(JstormAMConstant.REGISTRY_SUPERVISOR_PORT_LIST,supervisorPortStr);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_LOCAL_DIR,jstormHomePath);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_TYPE,containerType);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_CONTAINER,container.getId());
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_ZKROOT,zkRoot);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_ZKHOST,zkAddress);
                serviceRecord.set(JstormAMConstant.REGISTRY_JSTORM_ZKPORT,zkPort);
                serviceRecord.set(JstormAMConstant.REGISTRY_CONTAINER_PORT,containerPort);
                serviceRecord.set(JstormAMConstant.REGISTRY_STATUS,JstormAMConstant.REGISTRY_STATUS_RUNNING);
                serviceRecord.description = JstormAMConstant.REGISTRY_CONTAINER;
                serviceRecord.set(YarnRegistryAttributes.YARN_ID,yarnId);
                serviceRecord.set(YarnRegistryAttributes.YARN_PERSISTENCE, PersistencePolicies.CONTAINER);

                String containerPath = registryClient.getContainerPath(container);
                registryClient.mknode(containerPath);
                registryClient.bind(containerPath,serviceRecord);

                //环境变量的HashMap
                Map<String,String> shellEnvMap = new HashMap<String,String>();
                shellEnvMap.put("PATH",JstormAMConstant.ENV_PATH);
                shellEnvMap.put("JSTORM_CONF_DIR_CONF_YARN",containerHomePath);

                ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(
                        localResources, shellEnvMap, commands, null, allTokens.duplicate(), null);
                containerListener.addContainer(container.getId(), container);
                nmClientAsync.startContainerAsync(container, ctx);
           } catch (Exception e) {
                LOG.error("#### 启动进程失败",e);
            }
        }
    }

    /**
     * jstormAM退出时候的扫尾工作
     * @return
     */
    protected boolean finish() {
        // 等待运行完成
        while (JstormAMContext.isRunning) {
            try {
                Thread.sleep(JstormAMConstant.HEARTBEAT_TIME_INTERVAL);

                //todo: 可以在zk上更新这个应用的最新心跳时间,用来监控
                //todo: 路径为 /poseidon/yarn/application/${applicationId}/${applicationAttemptId}/heatbeat
            } catch (Exception ex) {
                LOG.error(ex);
            }
        }

        // 等待所有线程运行完毕
        for (Thread launchThread : launchThreads) {
            try {
                launchThread.join(JstormAMConstant.JOIN_THREAD_TIMEOUT);
            } catch (InterruptedException e) {
                LOG.info("Exception thrown in thread join: " + e.getMessage(),e);
            }
        }

        //当应用运行结束的时候,应当停掉所有运行的container
        LOG.error("#### 应用运行结束,开始停止所有container!");
        nmClientAsync.stop();

        //当应用运行结束,应当发送一个结束应用的信号给RM
        LOG.error("#### 应用运行结束,发送一个结束应用的信号给RM!");

        FinalApplicationStatus appStatus = FinalApplicationStatus.SUCCEEDED;
        String appMessage = "应用运行退出";
        boolean success = true;

        //todo:目前认为应用运行结束都是正常退出,后面会完善异常退出逻辑,区分正常退出和异常退出

        try {
            amRMClient.unregisterApplicationMaster(appStatus, appMessage, null);
        } catch (YarnException ex) {
            LOG.error("#### Failed to unregister application", ex);
        } catch (IOException e) {
            LOG.error("#### Failed to unregister application", e);
        }

        amRMClient.stop();
        return success;
    }

    /***
     * 判断container个数是不是足够
     * @param containerType
     * @return
     */
    private synchronized boolean compareContainerSize(Integer containerType){
        boolean result = true;
        if(containerType == JstormAMConstant.CONTAINER_TYPE_NIMBUS){
            //数量目前不够,需要执行新的container
            if(JstormAMContext.currentNimbusNum.intValue() < JstormAMContext.nimbusNum){
                JstormAMContext.currentNimbusNum.incrementAndGet();
                LOG.error("###true containerType:["+containerType+"],nimbusNum:["+JstormAMContext.nimbusNum+"],currentNimbusNum:["+JstormAMContext.currentNimbusNum.intValue()+"]");
            }
            //数量目前足够,不需要执行新的container
            else if(JstormAMContext.currentNimbusNum.intValue() == JstormAMContext.nimbusNum){
                result = false;
                LOG.error("###false containerType:["+containerType+"],nimbusNum:["+JstormAMContext.nimbusNum+"],currentNimbusNum:["+JstormAMContext.currentNimbusNum.intValue()+"]");

            }
        }
        else{
            //数量目前不够,需要执行新的container
            if(JstormAMContext.currentSupervisorNum.intValue() < JstormAMContext.supervisorNum){
                JstormAMContext.currentSupervisorNum.incrementAndGet();
                LOG.error("###true containerType:["+containerType+"],supervisorNum:["+JstormAMContext.supervisorNum+"],currentSupervisorNum:["+JstormAMContext.currentSupervisorNum.intValue()+"]");
            }
            //数量目前足够,不需要执行新的container
            else if(JstormAMContext.currentSupervisorNum.intValue() == JstormAMContext.supervisorNum){
                result = false;
                LOG.error("###false containerType:["+containerType+"],supervisorNum:["+JstormAMContext.supervisorNum+"],currentSupervisorNum:["+JstormAMContext.currentSupervisorNum.intValue()+"]");
            }
        }
        return result;
    }

}