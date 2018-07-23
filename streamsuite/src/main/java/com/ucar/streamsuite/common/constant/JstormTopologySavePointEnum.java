package com.ucar.streamsuite.common.constant;

/**
 * Description: jstorm topology savePoint类型的类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum JstormTopologySavePointEnum {

    /**
     * COMMON
     */
    COMMON (0,"COMMON"),

    /**
     * APP_ERROR
     */
    APP_ERROR(1,"APP异常"),

    /**
     * NIMBUS_ERROR
     */
    NIMBUS_ERROR(2,"NIMBUS异常"),

    /**
     * NIMBUS_CHANGE
     */
    NIMBUS_CHANGE(3,"NIMBUS变更"),

    /**
     * TOP_ERROR
     */
    TOP_ERROR(4,"TOP异常"),

    /**
     * APP_ERROR
     */
    ALL_ERROR(5,"执行恢复后仍失败");

    private int value;
    private String description;

    private JstormTopologySavePointEnum(int value, String desc) {
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
        for (JstormTopologySavePointEnum i : JstormTopologySavePointEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
