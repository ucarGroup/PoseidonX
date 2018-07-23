package com.ucar.streamsuite.common.hbase.proxy;

import com.ucar.streamsuite.common.hbase.vo.CommandsParameterVo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created on 2017/3/24.
 * Description : table 池
 */
public class HProxyTablePools {
    private static final Logger LOGGER = LoggerFactory.getLogger(HProxyTablePools.class);

    private static ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

    static {
        scheduledExecutorService.scheduleAtFixedRate(new Runnable(){
             @Override
             public void run() {
                 try{
                     HProxyTablePoolsHolder.getInstance().removeExpireTable();
                 } catch (Exception e) {
                     LOGGER.error("table池监控异常！", e);
                 }
             }
        }, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * 从池中拿出Htable
     */
    public static ProxyTable getHtable(String name,CommandsParameterVo vo) throws InterruptedException, IOException {
        return HProxyTablePoolsHolder.getInstance().getHTable(name,vo);
    }

    /**
     * 归还table
     * @param clusterName
     * @param name
     * @param table
     */
    public static void returnTable(String name,ProxyTable table) {
        HProxyTablePoolsHolder.getInstance().returnTable(name,table);
    }

}
