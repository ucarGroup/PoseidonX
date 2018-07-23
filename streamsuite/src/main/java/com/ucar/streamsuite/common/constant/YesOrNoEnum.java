package com.ucar.streamsuite.common.constant;

/**
 * Description: 表示是否的枚举
 * Created on 2018/1/18 下午4:33
 *
 */
public enum YesOrNoEnum {

    NO(0),

    YES(1);

    private int value;

    private YesOrNoEnum(int value) {
        this.value = value;
    }

    public int getValue() {
        return this.value;
    }
}
