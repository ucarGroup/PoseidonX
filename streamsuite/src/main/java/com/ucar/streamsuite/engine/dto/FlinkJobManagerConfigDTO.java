package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;

/**
 * Description: flink JobManager 信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class FlinkJobManagerConfigDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    public FlinkJobManagerConfigDTO() {
    }

    private String key;
    private String value;

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
