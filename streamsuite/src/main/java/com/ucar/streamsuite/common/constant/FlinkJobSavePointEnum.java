package com.ucar.streamsuite.common.constant;

/**
 * Description: flink Job savePoint类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum FlinkJobSavePointEnum {

    /**
     * COMMON
     */
    COMMON (0,"一般情况保存"),

    /**
     * APP_ERROR
     */
    APP_ERROR(1,"APP异常"),

    /**
     * JOB_ERROR
     */
    JOB_ERROR(2,"JOB异常"),

    /**
     * APP_ERROR
     */
    ALL_ERROR(3,"执行恢复后仍失败");

    private int value;
    private String description;

    private FlinkJobSavePointEnum(int value, String desc) {
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
        for (FlinkJobSavePointEnum i : FlinkJobSavePointEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
