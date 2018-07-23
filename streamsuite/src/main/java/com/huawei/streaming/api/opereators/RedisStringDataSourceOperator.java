package com.huawei.streaming.api.opereators;

import com.huawei.streaming.api.ConfigAnnotation;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.executor.operatorinfocreater.DataSourceInfoOperatorCreator;
import com.huawei.streaming.cql.executor.operatorinfocreater.OperatorInfoCreatorAnnotation;

/**
 * redis 数据源
 * Created on 2017/3/27 上午10:34:16
 *
 */
@OperatorInfoCreatorAnnotation(DataSourceInfoOperatorCreator.class)
public class RedisStringDataSourceOperator extends BaseDataSourceOperator{

    @ConfigAnnotation(StreamingConfig.REDIS_GROUPNAME)
    private String groupName;

    @ConfigAnnotation(StreamingConfig.REDIS_CONFIG)
    private String redisConfig;

    @ConfigAnnotation(StreamingConfig.REDIS_ZKSERVERS)
    private String zkservers;


    @ConfigAnnotation(StreamingConfig.REDIS_ZKPREFIX)
    private String zkprefix;

    /**
     * <默认构造函数>
     *
     * @param id
     * @param parallelNumber
     */
    public RedisStringDataSourceOperator(String id, int parallelNumber) {
        super(id, parallelNumber);
    }

    public String getGroupName() {
        return groupName;
    }
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
     * Setter for property 'zkprefix'.
     *
     * @param zkprefix Value to set for property 'zkprefix'.
     */
    public void setZkprefix(String zkprefix) {
        this.zkprefix = zkprefix;
    }
}
