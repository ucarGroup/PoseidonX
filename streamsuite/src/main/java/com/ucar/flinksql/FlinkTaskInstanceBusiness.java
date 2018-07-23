package com.ucar.flinksql;

import com.ucar.flinkcomponent.connectors.hbase.HBaseConstant;
import com.ucar.flinkcomponent.connectors.hbase.HBaseJsonTableSink;
import com.ucar.flinksql.component.*;
import com.ucar.flinksql.userFunction.HBaseTimeDesc;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.Kafka09JsonTableSink;
import org.apache.flink.streaming.connectors.kafka.MyKafkaJsonTableSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkFixedPartitioner;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.Types;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Description: flink sql 语法解析类
 * Created on 2018/5/7 上午10:42
 *
 */
public class FlinkTaskInstanceBusiness {

    private static final Logger logger = LoggerFactory.getLogger(FlinkTaskInstanceBusiness.class);

    private List<FlinkComponent> flinkComponentList = new ArrayList<FlinkComponent>();
    private Map<String,FlinkComponent> flinkComponentMap = new HashMap<String,FlinkComponent>();

    private Map<String,TableSink> tableSinkMap = new HashMap<String,TableSink>();

    private StreamExecutionEnvironment env;
    private StreamTableEnvironment tEnv;

    private FlinkTaskInstanceBusiness(){}


    /**
     * 唯一构建 FlinkSqlTaskInstanceBusiness 入口
     * @param flinkComponentList
     * @param flinkComponentMap
     * @return
     */
    public static FlinkTaskInstanceBusiness build(List<FlinkComponent> flinkComponentList, Map<String,FlinkComponent> flinkComponentMap){
        FlinkTaskInstanceBusiness flinkTaskInstanceBusiness = new FlinkTaskInstanceBusiness();

        flinkTaskInstanceBusiness.flinkComponentList = flinkComponentList;
        flinkTaskInstanceBusiness.flinkComponentMap = flinkComponentMap;

        return flinkTaskInstanceBusiness;
    }


    //创建flink任务
    public void createFlinkTask() throws Exception {
        //初始化环境
        initFlinkTaskEnv();

        //初始化用户自定义函数
        initUserFunction();

        //循环处理每个组件
        for(FlinkComponent flinkComponent : flinkComponentList) {

            //处理输入源 FlinkKafka010InputComponent
            if(flinkComponent instanceof FlinkKafka010InputComponent){
                dealFlinkKafka010InputComponent((FlinkKafka010InputComponent)flinkComponent);
            }
            //处理输出源 FlinkKafka09OutputComponent
            else if(flinkComponent instanceof FlinkKafka09OutputComponent){
                dealFlinkKafka09OutputComponent((FlinkKafka09OutputComponent)flinkComponent);
            }
            //处理输出源 FlinkFlexQOutputComponent
            else if(flinkComponent instanceof FlinkHBaseOutputComponent){
                dealFlinkHBaseOutputComponent((FlinkHBaseOutputComponent)flinkComponent);
            }
            //处理业务逻辑
            else if(flinkComponent instanceof FlinkTaskLogicComponent){
                dealFlinkTaskLogicComponent((FlinkTaskLogicComponent)flinkComponent);
            }
            //提交任务
            else if(flinkComponent instanceof FlinkSubmitTaskComponent){
                dealFlinkSubmitTaskComponent((FlinkSubmitTaskComponent)flinkComponent);
            }
            else{
                throw new FlinkSqlException("这是一个不支持的FlinkComponent.["+flinkComponent.getClass().getName()+"]");
            }
        }
    }



    /**
     * 初始化 flink 任务环境变量
     */
    private void initFlinkTaskEnv() {
        this.env = StreamExecutionEnvironment.getExecutionEnvironment();
        this.tEnv = TableEnvironment.getTableEnvironment(env);
    }

    /**
     * 初始化用户自定义函数
     */
    private void initUserFunction(){
        this.tEnv.registerFunction("hbase_time_desc",new HBaseTimeDesc());
    }

    /**
     * 构建 FlinkKafka010InputComponent 对应的业务逻辑
     * @param flinkComponent
     */
    private void dealFlinkKafka010InputComponent(FlinkKafka010InputComponent flinkComponent) {

        TypeInformation<Row> typeInfo = Types.ROW(
                flinkComponent.getColumnAliasNames(),
                flinkComponent.getColumnTypes()
        );

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers",flinkComponent.getBrokerHostPorts());
        kafkaProperties.setProperty("group.id",flinkComponent.getGroup());

        MyKafkaJsonTableSource kafkaTableSource = new MyKafkaJsonTableSource(
                flinkComponent.getTopic(),
                kafkaProperties,
                typeInfo,
                flinkComponent.getColumnNames());

        tEnv.registerTableSource(flinkComponent.getName(), kafkaTableSource);

    }

    /**
     * 构建 FlinkKafka09OutputComponent 对应的业务逻辑
     * @param flinkComponent
     */
    private void dealFlinkKafka09OutputComponent(FlinkKafka09OutputComponent flinkComponent) {

        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers",flinkComponent.getBrokerHostPorts());

        Kafka09JsonTableSink kafkaTableSink = new Kafka09JsonTableSink(
                flinkComponent.getTopic(),
                kafkaProperties,
                new FlinkFixedPartitioner());

        tableSinkMap.put(flinkComponent.getName(),kafkaTableSink);
    }

    /**
     * 构建 dealFlinkHBaseOutputComponent 对应的业务逻辑
     * @param flinkComponent
     */
    private void dealFlinkHBaseOutputComponent(FlinkHBaseOutputComponent flinkComponent) {

        TypeInformation<Row> typeInfo = Types.ROW(
                flinkComponent.getColumnNames(),
                flinkComponent.getDataTypeStrArray()
        );

        Properties sourceProperties = new Properties();
        sourceProperties.put(HBaseConstant.ZKHOST,flinkComponent.getZkHost());
        sourceProperties.put(HBaseConstant.ZKPORT,flinkComponent.getZkPort());
        sourceProperties.put(HBaseConstant.ZKPREFIX,flinkComponent.getZkPrefix());
        sourceProperties.put(HBaseConstant.TABLE_NAME,flinkComponent.getTableName());
        sourceProperties.put(HBaseConstant.COLFAMILY, flinkComponent.getColFamily());

        HBaseJsonTableSink hBaseJsonTableSink = new HBaseJsonTableSink(sourceProperties);

        tableSinkMap.put(flinkComponent.getName(),hBaseJsonTableSink);


    }

    /**
     * 处理具体sql业务逻辑
     * @param flinkComponent
     */
    private void dealFlinkTaskLogicComponent(FlinkTaskLogicComponent flinkComponent) {

        System.out.println(flinkComponent.getLogicSql());

        Table resultTable = tEnv.sql( flinkComponent.getLogicSql());
        TableSink tableSink =  tableSinkMap.get(flinkComponent.getTargetOutputComponentName());

        resultTable.printSchema();
        resultTable.writeToSink(tableSink);
    }

    /**
     * 处理提交任务的逻辑
     * @param flinkComponent
     */
    private void dealFlinkSubmitTaskComponent(FlinkSubmitTaskComponent flinkComponent) throws Exception {
        env.execute(flinkComponent.getApplicationName());
    }

}
