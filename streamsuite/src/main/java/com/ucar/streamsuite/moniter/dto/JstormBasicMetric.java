
package com.ucar.streamsuite.moniter.dto;

import backtype.storm.generated.MetricSnapshot;
import com.ucar.streamsuite.moniter.business.impl.JstormMetricBusiness;

import java.util.HashMap;
import java.util.Map;

public class JstormBasicMetric {

    // <metricName, value>
    protected Map<String, String> metrics = new HashMap<String, String>();

    public void setMetricValue(MetricSnapshot snapshot, String metricName){
        String value = JstormMetricBusiness.getMetricValue(snapshot);
        setValue(metricName, value);
    }

    protected void setValue(String metricName, String value) {
        metrics.put(metricName, value);
    }

    protected String getValue(String metricName) {
        return metrics.get(metricName);
    }

    public Map<String, String> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, String> metrics) {
        this.metrics = metrics;
    }
}
