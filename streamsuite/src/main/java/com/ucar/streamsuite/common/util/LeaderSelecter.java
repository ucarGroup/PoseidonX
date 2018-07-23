package com.ucar.streamsuite.common.util;

import com.ucar.streamsuite.common.constant.StreamContant;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderSelector;
import org.apache.curator.framework.recipes.leader.LeaderSelectorListenerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Description: zkLeader选举器
 * Created on 2018/1/18 下午4:33
 *
 */

public class LeaderSelecter extends LeaderSelectorListenerAdapter implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(LeaderSelecter.class);

    private static final LeaderSelecter instance;
    public static final LeaderSelecter getInstance() {
        return instance;
    }

    static{
        instance = new LeaderSelecter();
        instance.electionLeader();
    }

    /**
     * 执行选主
     */
    private void electionLeader(){
        cfClient = ZKUtil.getClient(StreamContant.HDFS_HADOOP_ZOOKEEPER);
        if(cfClient == null){
            LOGGER.error("LeaderSelecter electionLeader failed so cfClient is null, HDFS_HADOOP_ZOOKEEPER=" + StreamContant.HDFS_HADOOP_ZOOKEEPER);
            return ;
        }
        leaderSelector = new LeaderSelector(cfClient, StreamContant.ZOOKEEPER_LEADER_DIR, this);
        leaderSelector.autoRequeue();
        leaderSelector.start();
    }

    @Override
    public void takeLeadership(CuratorFramework curatorFramework) throws Exception {
        LOGGER.error(NetUtil.getLocalHostAddress() + " is leader ready running");
        isLeader = true;
        try {
            leaderLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error(NetUtil.getLocalHostAddress() + " is leader shuwdown");
        }
        isLeader = false;
    }

    @Override
    public void close() throws IOException {
        leaderSelector.close();
    }

    private LeaderSelector leaderSelector = null;

    private CuratorFramework cfClient = null;

    private CountDownLatch leaderLatch = new CountDownLatch(1);

    private boolean isLeader = false;

    public boolean isLeader() {
        return isLeader;
    }

}
