
package com.ucar.streamsuite.moniter.dto;

import java.io.Serializable;
import java.util.List;

public class FlinkVerticeMetricDto implements Serializable {

    private static final long serialVersionUID = -5729044331250315860L;

    private String id;
    private String name;
    private List<FlinkMetricDto> metricDtos;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<FlinkMetricDto> getMetricDtos() {
        return metricDtos;
    }

    public void setMetricDtos(List<FlinkMetricDto> metricDtos) {
        this.metricDtos = metricDtos;
    }
}
