package com.ucar.streamsuite.common.constant;

/**
 * Description: 审核状态
 * Created on 2018/1/18 下午4:33
 *
 */
public enum AuditStatusEnum {

    /**
     * 未审核
     */
    WAIT(0,"未审核"),

    /**
     * 审核通过
     */
    PASS(1,"审核通过"),

    /**
     * 审核驳回
     */
    REJECT(2,"审核驳回");

    private int value;
    private String description;

    private AuditStatusEnum(int value, String desc) {
        this.value = value;
        this.description = desc;
    }

    public int getValue() {
        return this.value;
    }

    public String getDescription() {
        return this.description;
    }

    public static String getDescription(int index) {
        for (AuditStatusEnum i : AuditStatusEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
