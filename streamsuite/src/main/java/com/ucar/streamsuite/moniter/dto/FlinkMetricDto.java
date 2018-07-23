
package com.ucar.streamsuite.moniter.dto;

import java.io.Serializable;

public class FlinkMetricDto implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    private String id;
    private String value;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }
}
