
package com.ucar.streamsuite.moniter.dto;

import backtype.storm.generated.ErrorInfo;
import backtype.storm.generated.MetricSnapshot;
import com.alibaba.jstorm.metric.MetricDef;

import com.alibaba.jstorm.utils.JStormUtils;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.ucar.streamsuite.moniter.business.impl.JstormMetricBusiness;


import java.util.*;

public class JstormComponentMetric extends JstormBasicMetric {
    protected String componentName;
    protected int parallel;
    protected String type;
    protected List<JstormErrorDto> errors;
    protected String sortedKey;

    // <metricName, <parentCompName, value>>
    protected Map<String, Map<String, String>> subMetricMap = new HashMap<String, Map<String, String>>();
    // <metricName@parentCompName, value>, this is used to iterate in jsp
    protected Map<String, String> subMetrics = new HashMap<String, String>();
    protected Set<String> parentComponent;

    public JstormComponentMetric() {}

    public JstormComponentMetric(String componentName) {
        this.componentName = componentName;
    }

    public JstormComponentMetric(String componentName, int parallel, String type) {
        this.componentName = componentName;
        this.parallel = parallel;
        this.type = type;
    }

    public void setMetricValue(MetricSnapshot snapshot, String parentComp, String metricName) {
        if (parentComp != null) {
            String value = JstormMetricBusiness.getMetricRawValue(snapshot);
            putSubMetricMap(metricName, parentComp, value);
        } else {
            String value = JstormMetricBusiness.getMetricValue(snapshot);
            setValue(metricName, value);
        }
    }

    public void mergeValue() {
        Set<String> set = new HashSet<String>();
        for (Map.Entry<String, Map<String, String>> entry : subMetricMap.entrySet()) {
            String metricName = entry.getKey();
            if (getValue(metricName) == null) {
                // only when the value is not be set yet, we set the merged value
                String value = merge(metricName, entry.getValue());
                setValue(metricName, value);
            }
            //add all the component names to the set
            set.addAll(entry.getValue().keySet());
            // fill the subMetrics , format <metricName@parentCompName, value>
            for(Map.Entry<String, String> en : entry.getValue().entrySet()){
                String compName = en.getKey();
                String v = en.getValue();
                String value;
                if(v.contains(".")){
                    value = JstormMetricBusiness.format(JStormUtils.parseDouble(v));
                }else{
                    value = JstormMetricBusiness.format(JStormUtils.parseLong(v));
                }
                subMetrics.put(metricName+"@"+compName, value);
            }
        }
        parentComponent = set;
    }

    protected String merge(String metricName, Map<String, String> list) {
        double value = 0d;
        if (metricName.equals(MetricDef.EMMITTED_NUM) || metricName.equals(MetricDef.ACKED_NUM)
                || metricName.equals(MetricDef.FAILED_NUM)) {
            for (String s : list.values()) {
                value += JStormUtils.parseDouble(s);
            }
            return JstormMetricBusiness.format((long) value);
        } else {
            for (String s : list.values()) {
                value += JStormUtils.parseDouble(s);
            }
            if (list.size() > 0) value = value / list.size();
            return JstormMetricBusiness.format(value);
        }
    }

    public void putSubMetricMap(String metricName, String parentComp, String value) {
        if (subMetricMap.containsKey(metricName)) {
            Map<String, String> map = subMetricMap.get(metricName);
            map.put(parentComp, value);
        } else {
            Map<String, String> map = new HashMap<String, String>();
            map.put(parentComp, value);
            subMetricMap.put(metricName, map);
        }
    }

    public void setParallel(int parallel) {
        this.parallel = parallel;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setComponentName(String componentName) {
        this.componentName = componentName;
    }

    public String getComponentName() {
        return componentName;
    }

    public int getParallel() {
        return parallel;
    }

    public String getType() {
        return type;
    }

    @JsonIgnore
    public Map<String, String> getSubMetrics() {
        return subMetrics;
    }

    @JsonIgnore
    public Set<String> getParentComponent() {
        return parentComponent;
    }

    public List<JstormErrorDto> getErrors() {
        return errors;
    }

    public void setErrors(List<ErrorInfo> errors) {
        if (errors == null){
            this.errors = null;
            return;
        }
        this.errors = new ArrayList<JstormErrorDto>();
        for (ErrorInfo info : errors){
            JstormErrorDto err = new JstormErrorDto(info.get_errorTimeSecs(), info.get_error());
            this.errors.add(err);
        }
    }

    @JsonIgnore
    public String getSortedKey() {
        return sortedKey;
    }

    public void setSortedKey(String sortedKey) {
        this.sortedKey = sortedKey;
    }
}
