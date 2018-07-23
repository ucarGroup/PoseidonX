
package com.ucar.streamsuite.moniter.dto;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.ucar.streamsuite.moniter.business.impl.JstormMetricBusiness;

public class JstormWorkerMetric extends JstormBasicMetric {
    private String host;
    private String port;
    private String topology;    //topology id

    public JstormWorkerMetric() {
    }

    public JstormWorkerMetric(String _host, String _port) {
        host = _host;
        port = _port;
    }

    public JstormWorkerMetric(String _host, String _port, String _topology) {
        host = _host;
        port = _port;
        topology = _topology;
    }

    public void setMetricValue(MetricSnapshot snapshot, String metricName) {
        if (metricName.equals(MetricDef.MEMORY_USED)) {
            String value = (long) snapshot.get_doubleValue() + "";
            setValue(metricName, value);

        } else {
            String value = JstormMetricBusiness.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }


    public String getHost() {
        return host;
    }

    public String getPort() {
        return port;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPort(String port) {
        this.port = port;
    }

    @JsonIgnore
    public String getTopology() {
        return topology;
    }

    public void setTopology(String topology) {
        this.topology = topology;
    }
}
