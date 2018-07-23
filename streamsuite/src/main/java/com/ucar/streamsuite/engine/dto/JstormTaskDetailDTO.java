package com.ucar.streamsuite.engine.dto;

import java.io.Serializable;

/**
 * Description: jstorm 任务运行时的详情信息的DTO
 * Created on 2018/1/18 下午4:33
 *
 */
public class JstormTaskDetailDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315880L;

    private String savepointTime;

    private Integer savepointType;

    private JstormClusterDTO jstormClusterDTO;

    private JstormTopologyDTO jstormTopologyDTO;

    public JstormClusterDTO getJstormClusterDTO() {
        return jstormClusterDTO;
    }

    public void setJstormClusterDTO(JstormClusterDTO jstormClusterDTO) {
        this.jstormClusterDTO = jstormClusterDTO;
    }

    public JstormTopologyDTO getJstormTopologyDTO() {
        return jstormTopologyDTO;
    }

    public void setJstormTopologyDTO(JstormTopologyDTO jstormTopologyDTO) {
        this.jstormTopologyDTO = jstormTopologyDTO;
    }

    public String getSavepointTime() {
        return savepointTime;
    }

    public void setSavepointTime(String savepointTime) {
        this.savepointTime = savepointTime;
    }

    public Integer getSavepointType() {
        return savepointType;
    }

    public void setSavepointType(Integer savepointType) {
        this.savepointType = savepointType;
    }
}
