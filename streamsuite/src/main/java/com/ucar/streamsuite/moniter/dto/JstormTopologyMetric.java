
package com.ucar.streamsuite.moniter.dto;

import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;
import com.ucar.streamsuite.moniter.business.impl.JstormMetricBusiness;

public class JstormTopologyMetric extends JstormBasicMetric {

    public void setMetricValue(MetricSnapshot snapshot, String metricName) {
        if (metricName.equals(MetricDef.MEMORY_USED)) {
            String value = (long) snapshot.get_doubleValue() + "";
            setValue(metricName, value);

        } else {
            String value = JstormMetricBusiness.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }
}
