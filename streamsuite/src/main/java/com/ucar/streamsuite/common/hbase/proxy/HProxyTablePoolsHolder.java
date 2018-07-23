package com.ucar.streamsuite.common.hbase.proxy;

import com.ucar.streamsuite.common.hbase.HBaseManager;
import com.ucar.streamsuite.common.hbase.vo.CommandsParameterVo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created on 2017/3/24.
 */
public class HProxyTablePoolsHolder {

    private static final Logger LOGGER = LoggerFactory.getLogger(HProxyTablePoolsHolder.class);

    private static volatile HProxyTablePoolsHolder instance;

    private final ConcurrentHashMap<String, ProxyTableHolder> poolsMap = new ConcurrentHashMap<String, ProxyTableHolder>();

    public static HProxyTablePoolsHolder getInstance() {
        if (instance == null) {
            synchronized (HProxyTablePoolsHolder.class) {
                if (instance != null) {
                    return instance;
                }
                HProxyTablePoolsHolder poolsHolder = new HProxyTablePoolsHolder();
                instance = poolsHolder;
            }
        }
        return instance;
    }

   /**
    * 获取 table
    */
    public ProxyTable getHTable(String name, CommandsParameterVo vo) throws InterruptedException, IOException {
        try{
            return getTableHolder(name).getHTable(name,vo);
        }catch(Exception e){
            LOGGER.error("getHTable error",e);
        }
        return null;
    }

    /**
     * 放回 table
     */
    public void returnTable(String name,ProxyTable table) {
        getTableHolder(name).returnTable(table);
    }

    /**
     * 移除过期的table
     */
    public void removeExpireTable() throws IOException {
        Iterator<Map.Entry<String, ProxyTableHolder>> ite = poolsMap.entrySet().iterator();
        while (ite.hasNext()) {
            Map.Entry<String, ProxyTableHolder> entry = ite.next();
            ProxyTableHolder tableHolder = entry.getValue();
            tableHolder.removeExpireTable();
        }
    }

    private ProxyTableHolder getTableHolder(String tableName) {
        ProxyTableHolder tableHolder = poolsMap.get(tableName);
        if (tableHolder == null) {
            poolsMap.putIfAbsent(tableName, new ProxyTableHolder());
        }
        return poolsMap.get(tableName);
    }

    private static class ProxyTableHolder{

        private static final TablePoolsConfig defaultConfig = new TablePoolsConfig();
        private final int takeTryTimes = 20;

        private volatile LinkedBlockingQueue<ProxyTable> queue;
        private volatile boolean queueInitialize;
        private AtomicLong currentTableCount;

        private volatile ReentrantLock lock = new ReentrantLock();

        //生成proxyTableHolder唯一id，作用:returnTable方法判断ProxyTable是否属于该ProxyTable创建，如果不是，则关闭ProxyTable
        private volatile String proxyTableID = UUID.randomUUID().toString();

        public ProxyTableHolder() {
            this.currentTableCount = new AtomicLong(0);
            this.queue = new LinkedBlockingQueue<ProxyTable>(defaultConfig.getMaxSize());
            this.queueInitialize = false;
        }

        /**
         * 获取 table
         * @param name
         * @param vo
         * @return
         * @throws InterruptedException
         * @throws IOException
         */
        public ProxyTable getHTable(String name, CommandsParameterVo vo) throws  IOException {
            if(lock.isLocked()) {
                //等待proxyTableHolder对象配置更新完毕
                while(lock.isLocked()) {
                    try{
                        Thread.sleep(200);
                    }catch(InterruptedException e) {
                        LOGGER.error("getHTable InterruptedException",e);
                    }
                }
            }
            if (!queueInitialize) {
                queueInitialize = true;
                addHTablePools(name, vo, defaultConfig.getCoreSize());
            }

            ProxyTable table = poll();
            if (table != null) {
                return table;
            }

            while (currentTableCount.get() < defaultConfig.getMaxSize()) {
                addHTablePools(name, vo, 1);
                table = poll();
                if (table != null) {
                    return table;
                }
            }

            table = take();
            return table;
        }

        /**
         * 放回 table
         * @param table
         */
        public void returnTable(ProxyTable table) {
            if(lock.isLocked()) {
                //等待proxyTableHolder对象配置更新完毕
                while(lock.isLocked()) {
                    try {
                        Thread.sleep(200);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
            if(table.getHolderUUID().equals(proxyTableID)){
                //标记table 失败，close table
                if (table != null && table.isFail()) {
                    currentTableCount.decrementAndGet();
                    try {
                        table.close();
                    } catch (IOException e) {
                        LOGGER.error(e.getMessage());
                    }
                    return;
                }
                offerForNoAdd(table);
            } else {
                //ProxyTable不属于该ProxyTableHolder创建，创建该ProxyTable的ProxyTableHolder已被从HTablePoolsHolder移除，需要将该ProxyTable关闭
                try{
                    table.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage(),e);
                }
            }
        }

        private void addHTablePools(String name, CommandsParameterVo vo, int addCount) throws IOException {
            Configuration configuration = HBaseManager.getConfiguration();
            for (int i = 0; i < addCount; i++) {
                ProxyTable table = new ProxyTable(configuration, Bytes.toBytes(name),proxyTableID);
                table.setAutoFlush(vo.isAutoFlush(), vo.isClearBufferOnFail());
                boolean result = offerForAdd(table);
                if (!result) {
                    break;
                }
            }
        }

        //放
        private boolean offer(ProxyTable table, boolean forAdd) {
            boolean result = queue.offer(table);
            if (forAdd && result) {
                currentTableCount.incrementAndGet();
            }
            return result;
        }

        private boolean offerForNoAdd(ProxyTable table) {
            return offer(table, false);
        }

        private boolean offerForAdd(ProxyTable table) {
            return offer(table, true);
        }

        //取
        private ProxyTable poll() {
            ProxyTable table = queue.poll();
            if (table != null) {
                table.setUpdateTime(System.currentTimeMillis());
            }
            return table;
        }

        private ProxyTable take() {
            ProxyTable table = null;
            for (int i = 0; i < takeTryTimes; i++) {
                table = tryPoll();
                if (table != null) {
                    break;
                }
            }

            if (table != null) {
                table.setUpdateTime(System.currentTimeMillis());
            } else {
                throw new RuntimeException("尝试" + takeTryTimes + "次依然没有拿到 hTable对象.");
            }
            return table;
        }

        private ProxyTable tryPoll() {
            ProxyTable table = null;
            try {
                table = queue.poll(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                LOGGER.error(e.getMessage(), e);
            }
            return table;
        }

        public void removeExpireTable() throws IOException {
            TablePoolsConfig config = defaultConfig;
            Iterator<ProxyTable> pIte = queue.iterator();
            while (pIte.hasNext()) {
                ProxyTable table = pIte.next();
                if (System.currentTimeMillis() - table.getUpdateTime() > config.getKeepAliveTime()) {
                    table.close();
                    pIte.remove();
                    currentTableCount.decrementAndGet();
                }
            }
        }
    }

    /**
     * ProxyTable 对象池的配置信息  队列的 初始大小，最大大小，最大保持时间
     */
    private static class TablePoolsConfig {
        private int maxSize = 100;
        private int coreSize = 5;
        //单位 ms
        private long keepAliveTime = 60000;
        public int getMaxSize() {
            return maxSize;
        }
        public int getCoreSize() {
            return coreSize;
        }
        public long getKeepAliveTime() {
            return keepAliveTime;
        }
    }
}
