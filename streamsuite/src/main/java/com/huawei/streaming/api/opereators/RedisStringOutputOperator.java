package com.huawei.streaming.api.opereators;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.config.StreamingConfig;

/**
 * 处理 redis数据的输出
 * Created on 2017/3/28 下午2:02:45
 *
 */
public class RedisStringOutputOperator extends InnerOutputSourceOperator{


    @ConfigAnnotation(StreamingConfig.REDIS_GROUPNAME)
    private String groupName;

    @ConfigAnnotation(StreamingConfig.REDIS_CONFIG)
    private String redisConfig;

    @ConfigAnnotation(StreamingConfig.REDIS_ZKSERVERS)
    private String zkservers;


    @ConfigAnnotation(StreamingConfig.REDIS_ZKPREFIX)
    private String zkprefix;

    @ConfigAnnotation(StreamingConfig.REDIS_NAMESPACE)
    private String namespace;

    @ConfigAnnotation(StreamingConfig.REDIS_EXPIRY)
    private String expiry;

    /**
     * <默认构造函数>
     *
     * @param id
     * @param parallelNumber
     */
    public RedisStringOutputOperator(String id, int parallelNumber) {
        super(id, parallelNumber);
    }

    /**
     * Getter for property 'groupName'.
     *
     * @return Value for property 'groupName'.
     */
    public String getGroupName() {
        return groupName;
    }

    /**
     * Setter for property 'groupName'.
     *
     * @param groupName Value to set for property 'groupName'.
     */
    public void setGroupName(String groupName) {
        this.groupName = groupName;
    }

    /**
     * Getter for property 'redisConfig'.
     *
     * @return Value for property 'redisConfig'.
     */
    public String getRedisConfig() {
        return redisConfig;
    }

    /**
     * Setter for property 'redisConfig'.
     *
     * @param redisConfig Value to set for property 'redisConfig'.
     */
    public void setRedisConfig(String redisConfig) {
        this.redisConfig = redisConfig;
    }

    /**
     * Getter for property 'zkservers'.
     *
     * @return Value for property 'zkservers'.
     */
    public String getZkservers() {
        return zkservers;
    }

    /**
     * Setter for property 'zkservers'.
     *
     * @param zkservers Value to set for property 'zkservers'.
     */
    public void setZkservers(String zkservers) {
        this.zkservers = zkservers;
    }

    /**
     * Getter for property 'zkprefix'.
     *
     * @return Value for property 'zkprefix'.
     */
    public String getZkprefix() {
        return zkprefix;
    }

    /**
     * Getter for property 'namespace'.
     *
     * @return Value for property 'namespace'.
     */
    public String getNamespace() {
        return namespace;
    }

    /**
     * Setter for property 'namespace'.
     *
     * @param namespace Value to set for property 'namespace'.
     */
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Setter for property 'zkprefix'.
     *
     * @param zkprefix Value to set for property 'zkprefix'.
     */
    public void setZkprefix(String zkprefix) {
        this.zkprefix = zkprefix;
    }

    /**
     * Getter for property 'expiry'.
     *
     * @return Value for property 'expiry'.
     */
    public String getExpiry() {
        return expiry;
    }

    /**
     * Setter for property 'expiry'.
     *
     * @param expiry Value to set for property 'expiry'.
     */
    public void setExpiry(String expiry) {
        this.expiry = expiry;
    }
}
