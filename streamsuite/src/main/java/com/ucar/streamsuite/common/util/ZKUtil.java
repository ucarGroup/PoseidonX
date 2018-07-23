package com.ucar.streamsuite.common.util;

import com.google.common.collect.Lists;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.RetryPolicy;
import org.apache.curator.ensemble.fixed.FixedEnsembleProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.retry.BoundedExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Description: zk的管理工具
 * Created on 2018/1/18 下午4:33
 *
 */
public class ZKUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(ZKUtil.class);

    /**
     * 保存连接对象的缓存
     */
    private static ConcurrentHashMap<String,CuratorFramework> connetcionStringToZkClient = new ConcurrentHashMap<String, CuratorFramework>();

    /**
     * 获得一个zkClient
     * @param zkConnetcionString
     * @return
     */
    public static CuratorFramework getClient(String zkConnetcionString) {
        if(StringUtils.isBlank(zkConnetcionString)){
            return null;
        }
        if(connetcionStringToZkClient.containsKey(zkConnetcionString)){
            return connetcionStringToZkClient.get(zkConnetcionString);
        }
        CuratorFramework curator = createCurator(zkConnetcionString);
        //如果第一次则启动
        if(connetcionStringToZkClient.putIfAbsent(zkConnetcionString,curator) == null){
            curator.start();
            return curator;
        }else{
            return connetcionStringToZkClient.get(zkConnetcionString);
        }
    }

    /**
     * 获得一个zkClient
     * @param zkHsots
     * @param zkPort
     * @return
     */
    public static CuratorFramework getClient(List<String> zkHsots, Integer zkPort) {
        if(CollectionUtils.isEmpty(zkHsots) || zkPort == null){
            return null;
        }
        List<String> zkConnetcionStrings = Lists.newArrayList();
        for (String zkAddres : zkHsots) {
            zkConnetcionStrings.add(zkAddres + ":" + zkPort);
        }
        String zkConnetcionString = StringUtils.join(zkConnetcionStrings,",");
        if(connetcionStringToZkClient.containsKey(zkConnetcionString)){
            return connetcionStringToZkClient.get(zkConnetcionString);
        }
        CuratorFramework curator = createCurator(zkConnetcionString);
        //如果第一次则启动
        if(connetcionStringToZkClient.putIfAbsent(zkConnetcionString,curator) == null){
            curator.start();
            return curator;
        }else{
            return connetcionStringToZkClient.get(zkConnetcionString);
        }
    }

    /**
     * 初始化一个Curator
     * @param zkConnetcionStrings
     * @return
     */
    private static CuratorFramework createCurator(String zkConnetcionStrings) {
        FixedEnsembleProvider ensembleProvider = new FixedEnsembleProvider(zkConnetcionStrings);
        int sessionTimeout = 60000;
        int connectionTimeout = 15000;
        int retryTimes = 5;
        int retryInterval = 1000;
        int retryCeiling = 60000;
        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();
        RetryPolicy retryPolicy = new BoundedExponentialBackoffRetry(retryInterval, retryCeiling, retryTimes);
        builder.ensembleProvider(ensembleProvider).connectionTimeoutMs(connectionTimeout).sessionTimeoutMs(sessionTimeout).retryPolicy(retryPolicy);
        CuratorFramework framework = builder.build();
        return framework;
    }

    /**
     * 关闭
     * @param curator
     */
    public static void closeClient(CuratorFramework curator){
        CloseableUtils.closeQuietly(curator);
    }

    /**
     * 获得状态
     * @param curator
     * @param path
     * @return
     * @throws Exception
     */
    private static Stat zkStat(CuratorFramework curator,String path) throws Exception {
        return curator.checkExists().forPath(path);
    }

    /**
     * 检查节点是否存在
     * @param curator
     * @param path
     * @return
     * @throws Exception
     */
    public static boolean pathExsit(CuratorFramework curator,String path) throws Exception {
        return zkStat(curator,path) != null ?true:false;
    }

    /**
     * 删除节点
     * @param curator
     * @param path
     * @param recursive
     * @param backgroundCallback
     * @throws Exception
     */
    public static void zkDelete(CuratorFramework curator, String path, boolean recursive, BackgroundCallback backgroundCallback) throws Exception {
        DeleteBuilder delete = curator.delete();
        if(recursive) {
            delete.deletingChildrenIfNeeded();
        }
        if(backgroundCallback != null) {
            delete.inBackground(backgroundCallback);
        }
        delete.forPath(path);
    }

    /**
     *
     * @描述：创建一个节点
     * @param path
     * @return void
     * @exception
     */
    public static void createPath(CuratorFramework client,String path,String pathValue) throws Exception{
        client.create().creatingParentsIfNeeded().forPath(path, pathValue.getBytes());
    }

    /**
     *
     * @描述：修改数据
     * @param path
     * @return void
     * @exception
     */
    public static void setData(CuratorFramework client,String path,String pathValue) throws Exception{
        client.setData().forPath(path, pathValue.getBytes());
    }

    /**
     *
     * @描述：获得数据
     * @param path
     * @return void
     * @exception
     */
    public static byte[] getData(CuratorFramework client,String path){
        try{
            if(!pathExsit(client, path)){
                return null;
            }
            return client.getData().forPath(path);
        }catch(Exception e){
            return null;
        }
    }

    /**
     *
     * @描述：获得子节点数据
     * @param path
     * @return void
     * @exception
     */
    public static List<String> getChildrens(CuratorFramework client,String path) throws Exception{
        if(!pathExsit(client, path)){
            return null;
        }
        return client.getChildren().forPath(path);
    }

    /**
     * 创建一个节点
     * @param zkHosts
     * @param zkPort
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static boolean createPath(List<String> zkHosts,Integer zkPort,String zkPath,String pathValue){
        CuratorFramework zkClient;
        zkClient = getClient(zkHosts, zkPort);
        if(zkClient == null){
            return false;
        }
        try{
            createPath(zkClient,zkPath,pathValue);
            return true;
        }catch(Exception e){
            LOGGER.error("ZKUtil createPath is error zkPath=" + zkPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort , e);
            return false;
        }
    }

    /**
     * 修改数据
     * @param zkHosts
     * @param zkPort
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static boolean setData(List<String> zkHosts,Integer zkPort,String zkPath,String pathValue){
        CuratorFramework zkClient;
        zkClient = getClient(zkHosts, zkPort);
        if(zkClient == null){
            return false;
        }
        try{
            setData(zkClient,zkPath,pathValue);
            return true;
        }catch(Exception e){
            LOGGER.error("ZKUtil setData is error zkPath=" + zkPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort , e);
            return false;
        }
    }

    /**
     * 检查zk集群上是否存在同名路径
     * @param zkHosts
     * @param zkPort
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static boolean pathExsit(List<String> zkHosts,Integer zkPort,String zkPath) throws Exception {
        CuratorFramework zkClient;
        zkClient = getClient(zkHosts, zkPort);
        if(zkClient == null){
            return false;
        }
        return pathExsit(zkClient,zkPath);
    }

    /**
     * 检查zk集群上是否存在同名路径，存在则删除
     * @param zkHosts
     * @param zkPort
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static boolean delerePath(List<String> zkHosts,Integer zkPort,String zkPath) throws Exception{
        CuratorFramework zkClient;
        zkClient = getClient(zkHosts, zkPort);
        if(zkClient == null){
            return false;
        }
        if(!pathExsit(zkHosts,zkPort,zkPath)){
            return false;
        }
        try{
            ZKUtil.zkDelete(zkClient,zkPath,true,null);
            return true;
        }catch(Exception e){
            LOGGER.error("ZKUtil delereClusterIfExsit is error zkPath=" + zkPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort , e);
            return false;
        }
    }

    /**
     * 获得数据
     * @param zkHosts
     * @param zkPort
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static String getData(List<String> zkHosts,Integer zkPort,String zkPath)  {
        CuratorFramework zkClient;
        zkClient = getClient(zkHosts, zkPort);
        if(zkClient == null){
            return null;
        }
        try{
            byte[] data = getData(zkClient,zkPath);
            if(data!=null){
                return new String(data);
            }
            return null;
        }catch(Exception e){
            LOGGER.error("ZKUtil getData is error zkPath=" + zkPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort , e);
            return null;
        }
    }

    /**
     * 获得数据
     * @param zkHosts
     * @param zkPort
     * @param zkPath
     * @return
     * @throws Exception
     */
    public static List<String> getChildrens(List<String> zkHosts,Integer zkPort,String zkPath)  throws Exception {
        CuratorFramework zkClient;
        zkClient = getClient(zkHosts, zkPort);
        if(zkClient == null){
            return null;
        }
        if(!pathExsit(zkHosts,zkPort,zkPath)){
            return null;
        }
        try{
            return getChildrens(zkClient,zkPath);
        }catch(Exception e){
            LOGGER.error("ZKUtil getChildrens is error zkPath=" + zkPath + " zkAddress=" + StringUtils.join(zkHosts,",") + " zkPort="+zkPort , e);
            return null;
        }
    }
}
