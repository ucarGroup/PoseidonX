package com.ucar.streamsuite.common.util;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.RMHAUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Description: 访问Yarn的工具类
 * Created on 2018/1/18 下午4:33
 *
 */
public class YarnClientProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(YarnClientProxy.class);

    private static YarnConfiguration conf;

    private static YarnClient yarnClient;

    static{
        startClient();
    }

    /**
     * 开始客户端
     */
    private static void startClient(){
        conf = new YarnConfiguration();
        yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();
    }

    public static Configuration getConf() {
        return conf;
    }

    /**
     * 获得container的最小分配内存
     * @return
     */
    public static String getConatinerMinMem()  {
       return getConf().get("yarn.scheduler.minimum-allocation-mb");
    }

    /**
     * 获得container的最大分配内存
     * @return
     */
    public static String getConatinerMaxMem()  {
        return getConf().get("yarn.scheduler.maximum-allocation-mb");
    }

    /**
     * 获得当前active的rmid
     * @return
     */
    public static String findActiveRMHAId()  {
        return RMHAUtils.findActiveRMHAId(conf);
    }

    /**
     * 获得当前active rm的地址
     * @return
     */
    public static String getActiveRMWebAddresses()  {
        String RMHAId = findActiveRMHAId();
        return getConf().get("yarn.resourcemanager.webapp.address." + RMHAId);
    }

    /**
     * 创建一个APP (未提交)
     * @return
     * @throws Exception
     */
    public static YarnClientApplication createApplication() throws Exception {
       return yarnClient.createApplication();
    }

    /**
     * 提交APP
     * @param appContext
     * @throws Exception
     */
    public static void submitApplication(ApplicationSubmissionContext appContext) throws Exception {
        yarnClient.submitApplication(appContext);
    }

    /**
     * kill APP
     * @param applicationId
     * @throws Exception
     */
    public static void killApplicationByAppId(String applicationId) throws Exception {
        if(StringUtils.isBlank(applicationId)){
            return;
        }
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        yarnClient.killApplication(appId);
    }

    /**
     * 根据APPID获得ApplicationReport
     * @param applicationId
     * @throws Exception
     */
    public static ApplicationReport getApplicationReportByAppId(String applicationId) {
        if(StringUtils.isBlank(applicationId)){
            return null;
        }
        ApplicationId appId = ConverterUtils.toApplicationId(applicationId);
        ApplicationReport report = null;
        try {
            report = yarnClient.getApplicationReport(appId);
        } catch (Exception e) {
            //LOGGER.error("YarnClientProxy getApplicationReportByAppId is error",e);
        }
        return report;
    }

    /**
     * 通过APP attempID获得ContainerReports
     * @param applicationAttemptId
     * @throws Exception
     */
    public static List<ContainerReport> getContainersByAppAttemptId(ApplicationAttemptId applicationAttemptId){
        if(applicationAttemptId == null){
            return null;
        }
        List<ContainerReport> containerReports = null;
        try {
            containerReports = yarnClient.getContainers(applicationAttemptId);
        } catch (Exception e) {
            //LOGGER.error("YarnClientProxy getContainersByAppAttemptId is error",e);
        }
        return containerReports;
    }

    /**
     * 监视APP还是否启动成功
     * @param appId
     * @param waitSeconds
     * @throws Exception
     */
    public static void monitorAppSubmitComplate(String appId,Integer waitSeconds) throws Exception {
        while (true) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
            }
            ApplicationReport report = YarnClientProxy.getApplicationReportByAppId(appId);
            if (report==null) {
                throw new Exception("启动 yarn 环境 Application Master 时发生异常!");
            }

            YarnApplicationState state = report.getYarnApplicationState();
            FinalApplicationStatus dsStatus = report.getFinalApplicationStatus();

            if (YarnApplicationState.FINISHED == state) {
                if (FinalApplicationStatus.SUCCEEDED != dsStatus) {
                    LOGGER.error("应用未能成功 FINISHED!!" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                    throw new Exception("应用未能成功 FINISHED!!" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                }
            } else if (YarnApplicationState.KILLED == state) {
                LOGGER.error("应用已被 KILLED!!" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                throw new Exception("应用已被 KILLED!!" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
            }else if (YarnApplicationState.FAILED == state) {
                LOGGER.error("应用执行失败 FAILED!!" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
                throw new Exception("应用执行失败 FAILED!!" + " YarnState=" + state.toString() + ", DSFinalStatus=" + dsStatus.toString());
            } else if (YarnApplicationState.RUNNING == state) {
                LOGGER.error("启动 yarn 环境 Application Master 时成功!!");
                return;
            } else {
                if (waitSeconds-- <= 0) {
                    YarnClientProxy.killApplicationByAppId(appId);
                    LOGGER.error("启动 yarn 环境 Application Master 时发生异常，应用已被终止!");
                    throw new Exception("启动 yarn 环境 Application Master 时发生异常，应用已被终止!");
                }
            }
        }
    }

}
