package com.ucar.streamsuite.engine.business;

import com.ucar.streamsuite.common.config.ConfigProperty;
import com.ucar.streamsuite.common.constant.ConfigKeyEnum;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.common.util.HdfsClientProxy;

import com.ucar.streamsuite.engine.constants.EngineContant;
import com.ucar.streamsuite.engine.dto.FlinkJobDTO;
import com.ucar.streamsuite.engine.dto.FlinkTaskConfigDTO;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.*;

/**
 * Description: flink任务的处理类
 * Created on 2018/1/18 下午4:33
 *
 *
 */
public class FlinkOnYarnBusiness {

    public static final Logger LOGGER = LoggerFactory.getLogger(FlinkOnYarnBusiness.class);

    private final static ExecutorService printMessageService = Executors.newFixedThreadPool(20);

    /**
     * 开始一个session
     *
     * -nm  taskName  -jm  jobmanager内存  -n taskmanager个数  -tm taskManger 内存数
     * @return
     */
    public static Process startYarnSession(FlinkTaskConfigDTO taskConfigDTO) throws Exception{
        Process rt = null;
        try{
            Vector<CharSequence> params = new Vector<CharSequence>(30);
            if(taskConfigDTO.getJobMangerMem()!=null){
                params.add("-jm " + taskConfigDTO.getJobMangerMem());
            }
            params.add("-tm " + taskConfigDTO.getTaskMangerMem());
            params.add("-n " + taskConfigDTO.getTaskMangerNum());
            params.add("-nm " + taskConfigDTO.getTaskName());
            params.add("-s " + taskConfigDTO.getSlots());
            params.add("-d");
            params.add("-Dhigh-availability.zookeeper.path.namespace=/" + taskConfigDTO.getTaskName());

            StringBuilder command = new StringBuilder();
            for (CharSequence str : params) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
            commands.add(EngineContant.FLINK_HOME + "/bin/yarn-session.sh");
            commands.add(command.toString());

            String sheelString = StringUtils.join(commands," ");
            LOGGER.error("############FlinkOnYarnBusiness startYarnSession exec begin sheelString="+sheelString);

            rt = Runtime.getRuntime().exec(sheelString);
        }catch(Exception e){
            LOGGER.error("FlinkOnYarnBusiness startYarnSession is error",e);
            if(rt!=null){
                rt.destroy();
            }
            throw e;
        }
        return rt;
    }

    /**
     * 停止任务 并返回停止时是否成功
     * @return
     */
    public static boolean stopJob(String appId,String jobId){
        boolean result = false;
        if(StringUtils.isBlank(appId) || StringUtils.isBlank(jobId)){
            return result;
        }

        FlinkJobDTO flinkJobDTO = FlinkWebClientBusiness.getFlinkJobDtoWithRetry(appId,jobId);
        if(flinkJobDTO == null){
            //任务已经停止的时候， 直接返回true
            return true;
        }

        LOGGER.error(" FlinkWebClientBusiness.yarnCancel begin result is="+ result);

        try{
            result = FlinkWebClientBusiness.yarnCancel(appId,jobId);
        }catch(Exception e){
            LOGGER.error("FlinkOnYarnBusiness stopTask is error",e);
        }

        LOGGER.error(" FlinkWebClientBusiness.yarnCancel end result is="+ result);

        if(result){
            return FlinkWebClientBusiness.waitJobCanceledWithRetry(appId,jobId);
        }else{
            return result;
        }
    }

    /**
     * 提交任务 并返回执行结果消息
     *
     * @return
     */
    public static Process startJob(FlinkTaskConfigDTO taskConfigDTO,String yarnAppId) throws Exception{
        Process rt = null;
        try{
            //替换掉根
            String jarName= taskConfigDTO.getProjectJarPath().replace(StreamContant.HDFS_PROJECT_PACKAGE_ROOT,"");
            String localJarFilePath = ConfigProperty.getConfigValue(ConfigKeyEnum.LOCAL_PROJECT_ITEM_DIR)+ jarName;
            if(!new File(localJarFilePath).exists()){
                FileUtils.touch(new File(localJarFilePath));
                HdfsClientProxy.downloadFileToLocal(localJarFilePath,taskConfigDTO.getProjectJarPath());
            }

            String jarFilePath = localJarFilePath;
            String mainClass = taskConfigDTO.getClassPath();

            Vector<CharSequence> params = new Vector<CharSequence>(30);
            params.add("-m yarn-cluster");
            params.add("-yid " + yarnAppId);
            params.add("-yz /" + taskConfigDTO.getTaskName());
            params.add("-c " + mainClass);
            if(taskConfigDTO.getParallelism()!=null){
                params.add("-p " + taskConfigDTO.getParallelism());
            }
            params.add("-d");

            StringBuilder command = new StringBuilder();
            for (CharSequence str : params) {
                command.append(str).append(" ");
            }

            List<String> commands = new ArrayList<String>();
            commands.add(EngineContant.FLINK_HOME + "/bin/flink run");
            commands.add(command.toString());
            commands.add(jarFilePath);
            if(StringUtils.isNotBlank(taskConfigDTO.getCustomParams())){
                commands.add(taskConfigDTO.getCustomParams());
            }

            String sheelString = StringUtils.join(commands," ");
            LOGGER.error("############FlinkOnYarnBusiness startJob exec begin sheelString="+sheelString);

            rt = Runtime.getRuntime().exec(sheelString);
        }catch(Exception e){
            LOGGER.error("FlinkOnYarnBusiness startJob is error",e);
            if(rt!=null){
                rt.destroy();
            }
            throw e;
        }
        return rt;
    }
}
