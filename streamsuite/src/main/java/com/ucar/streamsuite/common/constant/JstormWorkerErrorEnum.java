package com.ucar.streamsuite.common.constant;

/**
 * Description: WorkerErrorTypeEnum的类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum JstormWorkerErrorEnum {

    /**
     * 迁移
     */
    TRANSFER(1,"TRANSFER"),
    /**
     * 重启
     */
    RESTART(2,"RESTART"),
    /**
     * 异常
     */
    INACTIVE(3,"INACTIVE"),
    /**
     * 长时间starting
     */
    STARTING(4,"STARTING");

    private int value;
    private String description;

    private JstormWorkerErrorEnum(int value, String desc) {
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
        for (JstormWorkerErrorEnum i : JstormWorkerErrorEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
