package com.ucar.streamsuite.common.constant;

/**
 * Description: 引擎类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum EngineTypeEnum {

    /**
     * jstorm
     */
    JSTORM(0,"jstorm"),

    /**
     * flink
     */
    FLINK(1,"flink");

    private int value;
    private String description;

    private EngineTypeEnum(int value, String desc) {
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
        for (EngineTypeEnum i : EngineTypeEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
