package com.ucar.streamsuite.common.constant;

/**
 * Description: 任务提交类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum TaskSubmitTypeEnum {

    /**
     * 提交
     */
    SUBMIT(0,"提交"),

    /**
     * 自动恢复
     */
    RECOVRY(1,"自动恢复");

    private int value;
    private String description;

    private TaskSubmitTypeEnum(int value, String desc) {
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
        for (TaskSubmitTypeEnum i : TaskSubmitTypeEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
