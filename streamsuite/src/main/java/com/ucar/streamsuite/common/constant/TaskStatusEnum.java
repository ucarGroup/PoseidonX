package com.ucar.streamsuite.common.constant;

/**
 * Description: 任务状态
 * Created on 2018/1/18 下午4:33
 *
 */
public enum TaskStatusEnum {

    /**
     * 未开始
     */
    WAIT(0,"未开始"),

    /**
     * 运行中
     */
    RUNNING(1,"运行中"),

    /**
     * 异常中止
     */
    ERROR(2,"异常中止"),

    /**
     * 暂停执行
     */
    STOP(3,"暂停执行");

    private int value;
    private String description;

    private TaskStatusEnum(int value, String desc) {
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
        for (TaskStatusEnum i : TaskStatusEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
