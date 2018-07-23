package com.ucar.streamsuite.yarn.utils;

import com.ucar.streamsuite.yarn.appmaster.JstormAMConstant;
import com.ucar.streamsuite.yarn.appmaster.JstormAMContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;

import java.io.IOException;
import java.net.ServerSocket;

/**
 * jstorm AM 的工具类
 */
public class JstormAMUtil {

    private static final Log LOG = LogFactory.getLog(JstormAMUtil.class);

    //初始化可用端口扫描类
    private static PortScanner portScanner = new PortScanner();
    static{
        portScanner.setPortRange(JstormAMConstant.AM_PORT_RANGE);
    }

    /**
     * 检查端口是否可用
     * @param port
     * @return
     */
    public static boolean isPortAvailable(int port) {
        try {
            ServerSocket socket = new ServerSocket(port);
            socket.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * 在范围内查找可用端口
     */
    public static int getAvailablePort() throws Exception {
        return portScanner.getAvailablePort();
    }

    /**
     * 构建对于container的申请
     * @param containerType
     * @param mem
     * @return
     */
    public static AMRMClient.ContainerRequest setupContainerAskForRM(int containerType, int mem) {
        Priority pri = Records.newRecord(Priority.class);
        pri.setPriority(containerType);
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(mem);
        capability.setVirtualCores(1);
        AMRMClient.ContainerRequest request = new AMRMClient.ContainerRequest(capability, null, null, pri);
        LOG.error("#### 申请container:[" + request.toString()+"]");
        return request;
    }

    /**
     * 通过用户名和container信息获得containerHome
     * @param container
     * @param userName
     * @return
     */
    public static String getContainerHome(Container container, String userName){
        String tempDir = JstormAMContext.conf.get("hadoop.tmp.dir");
        return tempDir + "/nm-local-dir/usercache/"+userName
                + "/appcache/" +  container.getId().getApplicationAttemptId().getApplicationId()+ "/"
                + container.getId();
    }

    /**
     * 获得Container的类型
     * @param containerType
     * @return
     */
    public static String getContainerType(int containerType) {
        if(containerType == JstormAMConstant.CONTAINER_TYPE_NIMBUS){
            return JstormAMConstant.NAME_NIMBUS;
        } else{
            return JstormAMConstant.NAME_SUPERVISOR;
        }
    }
}
