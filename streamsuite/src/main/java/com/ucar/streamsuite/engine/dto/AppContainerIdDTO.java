package com.ucar.streamsuite.engine.dto;

import com.ucar.streamsuite.common.constant.JstormContainerTypeEnum;
import com.ucar.streamsuite.engine.business.YarnZkRegistryBusiness;
import com.ucar.streamsuite.engine.constants.YarnZkContant;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.registry.client.types.ServiceRecord;

import java.io.Serializable;

public class AppContainerIdDTO implements Serializable {

    private static final long serialVersionUID = -6729054331250315880L;

    private String taskName;
    private String applicationId;
    private String containerId;
    private String containerPath;

    private JstormContainerTypeEnum containerType;
    private ServiceRecord serviceRecord;

    public AppContainerIdDTO(){
    }

    public AppContainerIdDTO(String taskName, String applicationId, String containerId){
        this.taskName = taskName;
        this.applicationId = applicationId;
        this.containerId = containerId;
        this.containerPath = YarnZkRegistryBusiness.PathBuilder.containerPath(taskName,applicationId,containerId);
    }


    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public void setApplicationId(String applicationId) {
        this.applicationId = applicationId;
    }

    public void setContainerId(String containerId) {
        this.containerId = containerId;
    }

    public void setContainerPath(String containerPath) {
        this.containerPath = containerPath;
    }

    public void setContainerType(JstormContainerTypeEnum containerType) {
        this.containerType = containerType;
    }

    public void setServiceRecord(ServiceRecord serviceRecord) {
        this.serviceRecord = serviceRecord;
        if(serviceRecord!=null){
            String jstorm_type = serviceRecord.get(YarnZkContant.ZK_CONTAINER_TYPE);
            if(StringUtils.isNotBlank(jstorm_type) && jstorm_type.equals(YarnZkContant.ZK_CONTAINER_TYPE_NIMBUS)){
                this.setContainerType(JstormContainerTypeEnum.NIMBUS);
            }
            if(StringUtils.isNotBlank(jstorm_type) && jstorm_type.equals(YarnZkContant.ZK_CONTAINER_TYPE_SUPERVISOR)){
                this.setContainerType(JstormContainerTypeEnum.SUPERVISOR);
            }
        }
    }

    public ServiceRecord getServiceRecord() {
        return serviceRecord;
    }

    public JstormContainerTypeEnum getContainerType() {
        return containerType;
    }

    public String getTaskName() {
        return taskName;
    }

    public String getApplicationId() {
        return applicationId;
    }

    public String getContainerPath() {
        return containerPath;
    }

    public String getContainerId() {
        return containerId;
    }

}