package com.ucar.flinksql.component;

/**
 * Description: kafka 提交任务组件
 * Created on 2018/5/7 下午2:25
 *
 */
public class FlinkSubmitTaskComponent implements FlinkComponent {

    private String applicationName;


    public FlinkSubmitTaskComponent(String applicationName) {
        this.applicationName = applicationName;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public void setApplicationName(String applicationName) {
        this.applicationName = applicationName;
    }
}
