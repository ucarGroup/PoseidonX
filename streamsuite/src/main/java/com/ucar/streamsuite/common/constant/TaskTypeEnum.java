package com.ucar.streamsuite.common.constant;

/**
 * Description: 任务类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum TaskTypeEnum {

    /**
     * 监控项目任务
     */
    MONITOR(0,"监控任务");

    private int value;
    private String description;

    private TaskTypeEnum(int value, String desc) {
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
        for (TaskTypeEnum i : TaskTypeEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
