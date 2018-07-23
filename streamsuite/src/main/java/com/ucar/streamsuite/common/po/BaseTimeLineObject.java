package com.ucar.streamsuite.common.po;

import java.util.Date;

/**
 * Description: 所有拥有创建时间与修改时间的对象基类
 * Created on 2018/1/18 下午4:41
 *
 */
public class BaseTimeLineObject {
    /**
     * 创建时间
     */
    private Date createTime;

    /**
     * 修改时间
     */
    private Date modifyTime;

    @Override
    public String toString() {
        return "BaseTimeLineObject{" +
                "createTime=" + createTime +
                ", modifyTime=" + modifyTime +
                '}';
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
}
