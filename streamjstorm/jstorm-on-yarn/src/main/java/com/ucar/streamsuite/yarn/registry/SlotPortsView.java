package com.ucar.streamsuite.yarn.registry;

import com.ucar.streamsuite.yarn.appmaster.JstormAMConstant;
import org.apache.hadoop.registry.client.types.ServiceRecord;
import org.apache.hadoop.util.StringUtils;
import java.util.*;

public class SlotPortsView {

    private int minPort;
    private int maxPort;
    private RegistryClient registryClient;

    public static final Object LOCK = new Object();

    public SlotPortsView(RegistryClient registryClient) {
        this.registryClient = registryClient;
    }

    /**
     * 获取可用的nimbus端口
     *
     * @return
     */
    public int  getAvailablePort(String type) throws Exception {

        //todo: 该功能应该做个同步锁,后面添加

         String jstormType =  JstormAMConstant.REGISTRY_NIMBUS_PORT;
         int minPort = JstormAMConstant.NIMBUS_MIN_PORT;
         int maxPort = JstormAMConstant.NIMBUS_MAX_PORT;

         if(type.equals(JstormAMConstant.REGISTRY_TYPE_LOGVIEW)){
             jstormType =  JstormAMConstant.REGISTRY_TYPE_LOGVIEW;
              minPort = JstormAMConstant.LOGVIEW_MIN_PORT;
              maxPort = JstormAMConstant.LOGVIEW_MAX_PORT;
         }
        int resultPort = -1;
        synchronized (LOCK) {

            //拼装这个ip在zk的保存信息的记录
            String globalPath = registryClient.getGlobalPath();

            ServiceRecord globalRecord = registryClient.resolve(globalPath);

            if(globalRecord == null){
                registryClient.mknode(globalPath);
            }

            List<String> portList = new ArrayList<String>();

            //这个ip之前没有用过
            if (globalRecord == null) {
                globalRecord = new ServiceRecord();
            }
            //这个ip使用过
            else {
                String portListStr = globalRecord.get(jstormType);

                //如果字符串不为空
                if (portListStr != null && portListStr.trim().length() != 0) {
                    //用逗号分割
                    String[] portArray = portListStr.split(",");

                    for (String port : portArray) {

                        if (port != null && port.trim().length() != 0) {
                            portList.add(port);
                        }
                    }
                }
            }

            //遍历端口范围,寻找可以使用的端口
            //前提是所有需要使用的端口都在zk里面注册了
            for (int i = minPort; i < maxPort; i++) {
                //如果当前没有使用这个节点
                if(!portList.contains(String.valueOf(i))){
                    portList.add(String.valueOf(i));
                    resultPort = i;
                    break;
                }
            }

            //记录数据到zk
            globalRecord.set(jstormType,StringUtils.arrayToString(portList.toArray(new String[0])));
            registryClient.bind(globalPath,globalRecord);
        }
        return resultPort;
    }

    /**
     * 获取可用的supervisor端口数
     *
     * @param supervisorHost
     * @param slotCount
     * @return
     */
    public List<String>  getAvailableSupervisorList(String supervisorHost,int slotCount) throws Exception {

        //todo: 该功能应该做个同步锁,后面添加

        synchronized (LOCK) {
            List<String> resultPortList = new ArrayList<String>();
            //拼装这个ip在zk的保存信息的记录
            String hostPath = registryClient.getHostPath(supervisorHost);

            ServiceRecord hostRecord = registryClient.resolve(hostPath);

            List<String> portList = new ArrayList<String>();

            //这个ip之前没有用过
            if (hostRecord == null) {
                hostRecord = new ServiceRecord();
            }
            //这个ip使用过
            else {
                String portListStr = hostRecord.get(JstormAMConstant.REGISTRY_SUPERVISOR_PORT_LIST);
                //如果字符串不为空
                if (portListStr != null && portListStr.trim().length() != 0) {
                    //用逗号分割
                    String[] portArray = portListStr.split(",");

                    for (String port : portArray) {

                        if (port != null && port.trim().length() != 0) {
                            portList.add(port);
                        }
                    }
                }
            }

            int currentSize = slotCount;
            //遍历端口范围,寻找可以使用的端口
            //前提是所有需要使用的端口都在zk里面注册了
            for (int i = JstormAMConstant.SUPERVISOR_MIN_PORT; i < JstormAMConstant.SUPERVISOR_MAX_PORT; i++) {
                //如果当前没有使用这个节点
                if(!portList.contains(String.valueOf(i))){
                    portList.add(String.valueOf(i));
                    resultPortList.add(String.valueOf(i));
                    currentSize--;
                }
                if(currentSize <= 0){
                    break;
                }
            }

            //记录数据到zk
            hostRecord.set(JstormAMConstant.REGISTRY_SUPERVISOR_PORT_LIST, StringUtils.arrayToString(portList.toArray(new String[0])));
            registryClient.bind(hostPath,hostRecord);

            return resultPortList;
        }
    }


    public int getMinPort() {
        return minPort;
    }

    public void setMinPort(int minPort) {
        this.minPort = minPort;
    }

    public int getMaxPort() {
        return maxPort;
    }

    public void setMaxPort(int maxPort) {
        this.maxPort = maxPort;
    }
}
