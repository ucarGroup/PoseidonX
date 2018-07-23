package com.ucar.streamsuite.plugin;

import backtype.storm.GenericOptionsParser;
import backtype.storm.generated.SpoutSpec;
import backtype.storm.topology.TopologyBuilder;
import com.alibaba.fastjson.JSON;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.*;

/**
 * jstorm 管理 接口类
 * Created on 2017/4/25 下午2:51:19
 *
 */
public class JstormManager {

    private static final JstormManager manager = new JstormManager();
    public static Logger LOG = LoggerFactory.getLogger(JstormManager.class);
    private JstormManager(){}

    public static JstormManager getInstance(){
        return manager;
    }

    public  String  taskJar;
    public  String  path;


    public String getTaskJar() {
        return taskJar;
    }

    public void setTaskJar(String taskJar) {
        this.taskJar = taskJar;
    }

    public String getPath() {
        return path;
    }

    public void setPath(String path) {
        this.path = path;
    }

    public boolean submitTopology(TopologyBuilder top, String topName,String jstormMapJson) throws Exception {
        Map  stormConf=new HashMap();
        try{
            LOG.error("********0000jstormMapJson=" + jstormMapJson);
          jstormMapJson=new String(Base64.decode(jstormMapJson));
            LOG.error("********1111jstormMapJson=" + jstormMapJson);
          JstormParam stormjson = JSON.parseObject(jstormMapJson , JstormParam.class);
          String zkroot=stormjson.getZkroot();
          String zkAddress=  stormjson.getZkAddress();
          String spoutworkers=stormjson.getSpoutworkers();
          String blotworkers=stormjson.getBlotworkers();
          String topworkers  =stormjson.getTopworkers();
          String workermem=  stormjson.getWorkermem();
          String workerjvmparam=stormjson.getWorkerjvmparam();
          String supervisorlist=stormjson.getSupervisorlist();
            if(null!=supervisorlist && supervisorlist.trim().length()>1){
                List<String> supervisorIpList = new ArrayList<String>();

                String[] supervisorIpListArray = supervisorlist.split(",");
                for(String supervisorIp : supervisorIpListArray){
                    supervisorIpList.add(supervisorIp);
                }
                stormConf.put("isolation.scheduler.machines",supervisorIpList);
                LOG.error("********  isolation.scheduler.machines="+supervisorIpList.toString());
            }
            if(null !=zkroot && zkroot.trim().length()>0){
              stormConf.put("storm.zookeeper.root",zkroot);
          }
          if(null !=zkAddress && zkAddress.trim().length()>0){
              List zklist=new ArrayList<String>();
              if(zkAddress.indexOf(",")>-1){
                  String[] zkarray = zkAddress.split(",");
                  for(String zktemp:zkarray){
                      zklist.add(zktemp);
                  }
              }else{
                  zklist.add(zkAddress) ;
              }
              stormConf.put("storm.zookeeper.servers",zklist);
          }
            LOG.error("********  spoutworkers="+spoutworkers);
          if(null !=spoutworkers && spoutworkers.trim().length()>0){
              Map spoutEnterMap=new HashMap();
              String[] spoutarray= spoutworkers.split(";");
              Map<String, SpoutSpec> spoutmap = top.createTopology().get_spouts();
              for(String tmpspout: spoutarray){

                  String[]   spoutentry=   tmpspout.split(":");
                  spoutEnterMap.put(spoutentry[0],spoutentry[1]);
                  if(null != spoutmap.get(spoutentry[0])){
                      spoutmap.get(spoutentry[0]).get_common().set_parallelism_hint(Integer.valueOf(spoutentry[1]));
                      LOG.error("********  set spout parallelism ="+spoutentry[0]+" :"+spoutentry[1]);
                  }
                  LOG.error("********  setspout" + spoutentry[0] + spoutentry[1]);
              }
              stormConf.put("topology.spout.parallelism",spoutEnterMap);

          }
          if(null !=blotworkers && blotworkers.trim().length()>0){
              Map blotEnterMap=new HashMap();
              String[] blotarray= blotworkers.split(";");
              Map<String, backtype.storm.generated.Bolt> blotmap = top.createTopology().get_bolts();
              for(String tmpblot: blotarray){
                  String[]   blotentry=   tmpblot.split(":");
                  blotEnterMap.put(blotentry[0],blotentry[1]);
                  if(null != blotmap.get(blotentry[0])){
                      blotmap.get(blotentry[0]).get_common().set_parallelism_hint(Integer.valueOf(blotentry[1]));
                      LOG.error("********  set blot parallelism ="+blotentry[0]+" :"+blotentry[1]);
                  }
              }
              stormConf.put("topology.bolt.parallelism",blotEnterMap);
          }
          if(null !=workerjvmparam && workerjvmparam.trim().length()>0){
              stormConf.put("worker.gc.childopts",workerjvmparam);
          }
            if(null !=topworkers && topworkers.trim().length()>0){
                stormConf.put("topology.workers",topworkers);
            }
            if(null !=workermem && workermem.trim().length()>0){
                stormConf.put("worker.memory.size",workermem);
            }

            String stormConfString="";
            Iterator iter = stormConf.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                stormConfString+= entry.getKey();
                stormConfString+= entry.getValue();
                stormConfString+="|";
            }
            LOG.error("********111stormConfString=" + stormConfString);
      }catch (Exception e){
            LOG.error(e.getMessage(),e);
      }
        return this.submitTopology(top,topName,stormConf);
    }

    public boolean submitTopology(TopologyBuilder top, String topName, Map customMap) throws Exception {

        try{
            Map stormConf = new HashMap();
            if(customMap != null){
                stormConf.putAll(customMap);
            }
            if(null !=path && path.trim().length()>0){
                File file = new File(path);
                String[] files = file.list();
                Map<String, String> map = new HashMap<String, String>();
                List<String> list = new ArrayList<String>();
                for(String f:files){
                    //过滤掉base-integrate-jstorm jar包
                    if(f.startsWith("base-integrate-jstorm")){
                        continue ;
                    }
                    //过滤掉base-integrate-jstorm jar包
                    if(f.startsWith("jstorm-")){
                        continue ;
                    }
                    map.put(f, path+File.separator+f);
                    list.add(f);
                    if(f.equals(taskJar)){
                        System.setProperty("storm.jar", path+File.separator+f);
                    }
                }
                stormConf.put(GenericOptionsParser.TOPOLOGY_LIB_NAME, list);
                stormConf.put(GenericOptionsParser.TOPOLOGY_LIB_PATH, map);
            }


            String stormConfString="";
            Iterator iter = stormConf.entrySet().iterator();
             while (iter.hasNext()) {
                Map.Entry entry = (Map.Entry) iter.next();
                 stormConfString+= entry.getKey();
                 stormConfString+= entry.getValue();
                 stormConfString+="|";
               }
            LOG.error("*************stormConfString=" + stormConfString);
            StormSubmitterForUCar.submitTopology(topName, stormConf, top.createTopology());

        }catch (Exception e){
            LOG.error(e.getMessage(),e);
        }finally{
            Properties pr = System.getProperties();
            pr.remove("storm.conf.file");
            pr.remove("storm.jar");
        }
        return true ;
    }

}
