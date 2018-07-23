package com.ucar.flinksql;

/**
 * Description: 所有关键词都是小写
 * Created on 2018/5/7 上午9:52
 *
 */
public class ConstantForSQL {

    public final static String KEYWORD_GlobalProperties = "globalproperties ";
    public final static String KEYWORD_CreateInputStream = "create input stream ";
    public final static String KEYWORD_CreateOutputStream = "create output stream ";
    public final static String KEYWORD_InsertIntoStream = "insert into stream ";
    public final static String KEYWORD_SubmitApplication = "submit application ";

    public final static String KEYWORD_Source = "source";
    public final static String KEYWORD_Properties = "properties";
    public final static String KEYWORD_SINK = "sink";


    public final static String KEYWORD_KAFKA010Input = "kafka010input";
    public final static String KEYWORD_KAFKA09Output = "kafka09output";

    public final static String KEYWORD_FlexQInput = "flexqinput";
    public final static String KEYWORD_FlexQOutput = "flexqoutput";

    public final static String LEFT_PARENTHESIS = "(";
    public final static String RIGHT_PARENTHESIS = ")";
    public final static String EQUAL = "=";
    public final static String COMMA = ",";
    public final static String SINGLE_QUOTATION  = "'";
    public final static String DOUBLE_QUOTATION  = "\"";



    public final static String COLUMN_NAME = "columnNameArray";
    public final static String DATA_TYPE = "dataTypeArray";

    public final static String BROKER = "brokers";
    public final static String ZOOKEEPER = "zookeepers";
    public final static String PREFIX = "prefix";
    public final static String TOPIC = "topic";
    public final static String GROUPID = "groupid";
    public final static String FORMAT = "format";//数据格式,默认为normal, 还一种格式是 datax,用于datax系统发出的同步数据的格式


    public final static String FORMAT_NORMAL = "normal";
    public final static String FORMAT_DATAX = "datax";


    //敏感词的标志,例如 数据source 的 字段名为 timestamp,这个是关键词,不能用,
    // 所以在 datasource的column写成 timestamp__$__mytime ,后面用mytime替代timestamp 使用
    public final static String COL_NAME_KEY_SIGN = "__&__";


    //处理dataX 数据发过来的列信息,参考FlexQETLTableSource
    public final static String ETL_ROW_LIST = "dbtablerowcellvolist";
    public final static String ETL_ROW_BEFORE = "row_before_";
    public final static String ETL_ROW_AFTER= "row_after_";
}
