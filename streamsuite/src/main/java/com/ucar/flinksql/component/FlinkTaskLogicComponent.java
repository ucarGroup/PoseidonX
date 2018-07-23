package com.ucar.flinksql.component;

/**
 * Description: kafka 业务逻辑组件
 * Created on 2018/5/7 下午2:39
 *
 */
public class FlinkTaskLogicComponent implements FlinkComponent{

    private String targetOutputComponentName;
    private String logicSql;

    public FlinkTaskLogicComponent(String targetOutputComponentName, String logicSql) {
        this.targetOutputComponentName = targetOutputComponentName;
        this.logicSql = logicSql;
    }

    public String getTargetOutputComponentName() {
        return targetOutputComponentName;
    }

    public void setTargetOutputComponentName(String targetOutputComponentName) {
        this.targetOutputComponentName = targetOutputComponentName;
    }

    public String getLogicSql() {
        return logicSql;
    }

    public void setLogicSql(String logicSql) {
        this.logicSql = logicSql;
    }
}
