package com.ucar.streamsuite.common.hbase.vo;

import java.io.Serializable;

public class FiltersOperatorVo implements Serializable {

    private static final long serialVersionUID = -4070473745560216683L;

    public enum Operator {
        /** !AND */
        MUST_PASS_ALL,
        /** !OR */
        MUST_PASS_ONE
    }

}
