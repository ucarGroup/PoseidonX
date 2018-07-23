package com.ucar.streamsuite.common.util;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;


/**
 * Description: zk 的分布式可重入锁
 * Created on 2018/1/18 下午4:33
 *
 */
public class DistributedReentrantLock {

    private InterProcessMutex lock;
    // 锁路径fullpath
    private final String lockPath;

    public DistributedReentrantLock(CuratorFramework client,String lockPath) {
        // 参数验证
        if (client == null || StringUtils.isBlank(lockPath)) {
            throw new IllegalArgumentException("DistributedReentrantLock argument is invalid. CuratorFramework=" + client + ", lockPath=" + lockPath);
        }
        // 锁路径lockPath
        this.lockPath = lockPath;
        // 锁对象创建
        lock = new InterProcessMutex(client, lockPath);
    }

    /**
     * 阻塞方法，直到获取锁（可重入）
     * 
     * @throws Exception ZK errors, connection interruptions
     */
    public void tryLock() throws Exception {
        lock.acquire();
    }

    /**
     * 在指定时间内获取锁（可重入）<br>
     * 超时将返回false
     * 
     * @param time time to wait
     * @param unit time unit
     * @return true if the mutex was acquired, false if not
     * @throws Exception ZK errors, connection interruptions
     */
    public boolean tryLock(long time, TimeUnit unit) throws Exception {
        return lock.acquire(time, unit);
    }

    /**
     * 是否拥有该锁
     * 
     * @return true/false
     */
    public boolean isLocked() {
        return lock.isAcquiredInThisProcess();
    }

    /**
     * 释放锁。每次获取锁后都需要对应释放一次。
     * 
     * @throws Exception ZK errors, interruptions, current thread does not own
     *         the lock
     */
    public void unlock() throws Exception {
        if (isLocked()) {
            lock.release();
        }
    }

    /**
     * 释放所有锁资源。
     * 
     * @throws Exception ZK errors, interruptions, current thread does not own
     *         the lock
     */
    public void releaseAll() throws Exception {
        // 释放所有锁资源
        while (lock.isAcquiredInThisProcess()) {
            lock.release();
        }
        lock = null;
    }

    /**
     * 获取锁路径
     * 
     * @return 全路径
     */
    public String getLockPath() {
        return lockPath;
    }
}
