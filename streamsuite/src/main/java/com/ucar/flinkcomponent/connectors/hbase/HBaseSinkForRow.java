package com.ucar.flinkcomponent.connectors.hbase;

import com.ucar.streamsuite.common.hbase.CommonHbaseRecord;
import com.ucar.streamsuite.common.hbase.HBaseManager;
import com.ucar.streamsuite.common.hbase.util.HBaseRecordUtils;
import com.ucar.streamsuite.common.hbase.util.HBaseUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

import org.apache.hadoop.conf.Configuration;

/**
 * Description: 支持写数据到hbase
 * Created on 2018/6/12 上午10:30
 *
 *
 */
public class HBaseSinkForRow extends RichSinkFunction {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(HBaseSinkForRow.class);

    private String[] fieldNames;
    private TypeInformation[] fieldTypes;

    private String zkHost;
    private String zkPort;
    private String zkPrefix;
    private String tableName;
    private String colFamily;

    private Configuration configuration;

    public HBaseSinkForRow(Properties properties, String[] fieldNames, TypeInformation[] fieldTypes){

        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;

        this.zkHost = properties.getProperty(HBaseConstant.ZKHOST);
        this.zkPort = properties.getProperty(HBaseConstant.ZKPORT);
        this.zkPrefix = properties.getProperty(HBaseConstant.ZKPREFIX);
        this.tableName = properties.getProperty(HBaseConstant.TABLE_NAME);
        this.colFamily = properties.getProperty(HBaseConstant.COLFAMILY);

        HBaseManager.initForFlinkSQL(properties);
    }


    @Override
    public void invoke(Object value) throws Exception {
        if(value == null){
            LOG.error("###### HBaseSinkForRow#invoke msg is null!");
            return ;
        }

        Row valueRow = (Row)value;

        //输出的数据第一列默认为rowKey
        String rowKey = String.valueOf(valueRow.getField(0));

        CommonHbaseRecord commonHbaseRecord = new CommonHbaseRecord(tableName,colFamily,rowKey);

       for(int i = 1;i<fieldNames.length;i++){
           String cellValue = String.valueOf(valueRow.getField(i));
           commonHbaseRecord.addHbaseCell(fieldNames[i], HBaseUtils.getBytes(cellValue));
       }

        try {
            HBaseRecordUtils.send(commonHbaseRecord,false);
        } catch (Exception e) {
            LOG.error("#### HBaseSinkForRow#send is fault",e);
            throw e;
        }
    }
}
