package com.ucar.streamsuite.common.constant;

/**
 * Description: Container的类型
 * Created on 2018/1/18 下午4:33
 *
 */
public enum JstormContainerTypeEnum {

    /**
     * NIMBUS
     */
    NIMBUS(0,"NIMBUS"),

    /**
     * SUPERVISOR
     */
    SUPERVISOR(1,"SUPERVISOR");

    private int value;
    private String description;

    private JstormContainerTypeEnum(int value, String desc) {
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
        for (JstormContainerTypeEnum i : JstormContainerTypeEnum.values()) {
            if (i.getValue() == index) {
                return i.description;
            }
        }
        return "";
    }
}
