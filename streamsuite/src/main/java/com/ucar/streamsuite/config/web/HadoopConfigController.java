package com.ucar.streamsuite.config.web;

import com.ucar.streamsuite.common.util.HdfsClientProxy;
import com.ucar.streamsuite.common.util.YarnClientProxy;
import com.ucar.streamsuite.config.dto.ConfigDTO;
import org.apache.hadoop.conf.Configuration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.List;

/**
 * Description: hadoop配置信息控制器
 * Created on 2018/2/1 上午11:46
 *
 *
 */
@Controller
@RequestMapping("/config/hadoop")
public class HadoopConfigController {

    private String[] CORE_CONF_KEY = {"fs.default.name","fs.defaultFS","hadoop.tmp.dir","ha.zookeeper.quorum","ha.zookeeper.parent-znode"};

    private String[] HDFS_CONF_KEY = {"dfs.nameservices","dfs.replication","ha.zookeeper.quorum",
                                       "dfs.namenode.rpc-address.${dfs.nameservices}.nn1","dfs.namenode.rpc-address.${dfs.nameservices}.nn2",
                                       "dfs.namenode.http-address.${dfs.nameservices}.nn1","dfs.namenode.http-address.${dfs.nameservices}.nn2",
                                       "dfs.namenode.shared.edits.dir","dfs.ha.fencing.ssh.private-key-files","dfs.journalnode.edits.dir"};

    private String[] YARN_CONF_KEY = {"yarn.resourcemanager.cluster-id","hadoop.registry.zk.quorum",
                                      "yarn.resourcemanager.hostname.rm1","yarn.resourcemanager.hostname.rm2",
                                      "yarn.resourcemanager.webapp.address.rm1","yarn.resourcemanager.webapp.address.rm2",
                                      "yarn.resourcemanager.ha.automatic-failover.zk-base-path",
                                      "yarn.nodemanager.resource.memory-mb","yarn.scheduler.minimum-allocation-mb",
                                      "yarn.nodemanager.resource.cpu-vcores","yarn.scheduler.maximum-allocation-mb"
    };


    @ResponseBody
    @RequestMapping(value = "/view", method = RequestMethod.POST)
    public List<ConfigDTO> view(HttpServletRequest request, HttpServletResponse response) {

        //读取hadoop配置,类型为 core,hdfs 和 yarn
        String type = request.getParameter("configType");
        List<ConfigDTO> list = this.getConfigInfo(type);
        return  list;

    }

    /**
     * 读取 hadoop 配置信息
     * @param type
     * @return
     */
    private List<ConfigDTO> getConfigInfo(String type) {

        Configuration configuration = HdfsClientProxy.getConf();
        String nameServices = configuration.get("dfs.nameservices");

        if(nameServices == null){
            nameServices = "";
        }

        List<ConfigDTO> list = new ArrayList<ConfigDTO>();

        String[] configKeys = CORE_CONF_KEY;

        if("hdfs".equals(type)){
            configKeys = HDFS_CONF_KEY;
        }
        else if("yarn".equals(type)){
            configKeys = YARN_CONF_KEY;
            configuration = YarnClientProxy.getConf();
        }
        else if("core".equals(type)){
            ConfigDTO configDTO = new ConfigDTO();
            configDTO.setConfigName("HADOOP_USER_NAME");
            configDTO.setConfigValue(System.getProperty("HADOOP_USER_NAME"));
            list.add(configDTO);
        }


        for(String key : configKeys){

            key = key.replace("${dfs.nameservices}",nameServices);
            String configValue = configuration.get(key);
            ConfigDTO configDTO = new ConfigDTO();
            configDTO.setConfigName(key);
            configDTO.setConfigValue(configValue);

            list.add(configDTO);
        }

        return list;

    }

}
