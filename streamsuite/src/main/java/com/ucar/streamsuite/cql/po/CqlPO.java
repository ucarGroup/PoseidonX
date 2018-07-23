package com.ucar.streamsuite.cql.po;

import com.ucar.streamsuite.common.po.BaseTimeLineObject;

import java.io.Serializable;

/**
 * Description: cql 脚本
 * Created on 2018/3/7 下午5:01
 *
 */
public class CqlPO extends BaseTimeLineObject implements Serializable {

    private static final long serialVersionUID = -5729044331250314760L;

    /**
     * 任务id（自增id）
     */
    private Integer id;

    /**
     * cql脚本名字
     */
    private String cqlName;

    /**
     * cql脚本名字
     */
    private String cqlText;

    /**
     * cql脚本类型  0 jstorm 1 flink
     */
    private Integer cqlType;

    /**
     * cql脚本备注
     */
    private String cqlRemark;

    /**
     * cql脚本状态
     */
    private Integer cqlStatue;

    /**
     * 任务创建人(任务信息的创建人)
     */
    private String creatorUserName;

    /**
     * 任务修改人(最近一次编辑任务信息的修改人)
     */
    private String modifyUserName;

    /**
     * 用户组
     */
    private Integer userGroupId;

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getCqlName() {
        return cqlName;
    }

    public void setCqlName(String cqlName) {
        this.cqlName = cqlName;
    }

    public String getCqlText() {
        return cqlText;
    }

    public void setCqlText(String cqlText) {
        this.cqlText = cqlText;
    }

    public String getCqlRemark() {
        return cqlRemark;
    }

    public void setCqlRemark(String cqlRemark) {
        this.cqlRemark = cqlRemark;
    }

    public Integer getCqlStatue() {
        return cqlStatue;
    }

    public void setCqlStatue(Integer cqlStatue) {
        this.cqlStatue = cqlStatue;
    }

    public String getCreatorUserName() {
        return creatorUserName;
    }

    public void setCreatorUserName(String creatorUserName) {
        this.creatorUserName = creatorUserName;
    }

    public String getModifyUserName() {
        return modifyUserName;
    }

    public void setModifyUserName(String modifyUserName) {
        this.modifyUserName = modifyUserName;
    }

    public Integer getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(Integer userGroupId) {
        this.userGroupId = userGroupId;
    }

    public Integer getCqlType() {
        return cqlType;
    }

    public void setCqlType(Integer cqlType) {
        this.cqlType = cqlType;
    }
}
