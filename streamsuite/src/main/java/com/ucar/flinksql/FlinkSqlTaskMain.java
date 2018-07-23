package com.ucar.flinksql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description:
 * Created on 2018/5/5 下午3:11
 *
 */
public class FlinkSqlTaskMain implements java.io.Serializable{

     private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTaskMain.class);
     private static final long serialVersionUID = 3L;


     public static void  main(String[] args) throws Exception {

         String originalSql = args[0];
         logger.error("flink sql original:["+originalSql+"] ");
         String sql = FlinkSqlUtils.getFromBase64(originalSql);
         logger.error("flink sql :["+sql+"] ");

         //分析sql文本,生成组件信息
         FlinkSqlTextBusiness flinkSqlBusiness = FlinkSqlTextBusiness.build(sql);
         flinkSqlBusiness.initSql();

         //分析生成的组件信息,创建flink任务
         FlinkTaskInstanceBusiness flinkTaskInstanceBusiness = FlinkTaskInstanceBusiness.build(flinkSqlBusiness.getFlinkComponentList(),flinkSqlBusiness.getFlinkComponentMap());

         //创建flink任务并提交
         flinkTaskInstanceBusiness.createFlinkTask();

         logger.error("FlinkSqlTaskMain#complete");
     }
}
