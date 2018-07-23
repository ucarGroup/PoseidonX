package com.ucar.streamsuite.cql.dto;

import java.io.Serializable;
import java.util.Date;

/**
 * Description: cql的dto
 * Created on 2018/3/12 下午2:00
 *
 */
public class CqlDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;


    /**
     * 任务id（自增id）
     */
    private Integer id;

    /**
     * cql脚本名字
     */
    private String cqlName;

    /**
     * cql脚本内容
     */
    private String cqlText;

    /**
     * cql脚本类型  0 jstorm 1 flink
     */
    private Integer cqlType;

    /**
     * cql脚本内容
     */
    private String cqlTextFirstLine;

    /**
     * cql脚本备注
     */
    private String cqlRemark;

    /**
     * cql脚本状态
     */
    private Integer cqlStatue;

    /**
     * cql脚本状态
     */
    private String cqlStatueStr;

    /**
     * 任务创建人(任务信息的创建人)
     */
    private String creatorUserName;

    /**
     * 任务修改人(最近一次编辑任务信息的修改人)
     */
    private String modifyUserName;

    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date modifyTime;

    /**
     * 用户组ID
     * @return
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

    public String getCqlStatueStr() {
        return cqlStatueStr;
    }

    public void setCqlStatueStr(String cqlStatueStr) {
        this.cqlStatueStr = cqlStatueStr;
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

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }

    public Date getModifyTime() {
        return modifyTime;
    }

    public void setModifyTime(Date modifyTime) {
        this.modifyTime = modifyTime;
    }

    public String getCqlTextFirstLine() {
        return cqlTextFirstLine;
    }

    public void setCqlTextFirstLine(String cqlTextFirstLine) {
        this.cqlTextFirstLine = cqlTextFirstLine;
    }

    public Integer getCqlType() {
        return cqlType;
    }

    public void setCqlType(Integer cqlType) {
        this.cqlType = cqlType;
    }

    public Integer getUserGroupId() {
        return userGroupId;
    }

    public void setUserGroupId(Integer userGroupId) {
        this.userGroupId = userGroupId;
    }
}
