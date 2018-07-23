package com.ucar.flinksql;

import com.alibaba.fastjson.JSONObject;
import com.ucar.flinkcomponent.connectors.hbase.HBaseConstant;
import com.ucar.flinksql.component.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Description: flink sql 语法解析类
 * Created on 2018/5/7 上午10:42
 *
 */
public class FlinkSqlTextBusiness {

    private static final Logger logger = LoggerFactory.getLogger(FlinkSqlTextBusiness.class);

    private String fullSql;
    private List<String> sqlList = new ArrayList<String>();

    private List<FlinkComponent> flinkComponentList = new ArrayList<FlinkComponent>();
    private Map<String,FlinkComponent> flinkComponentMap = new HashMap<String,FlinkComponent>();


    private FlinkSqlTextBusiness(){}

    /**
     * 唯一构建 FlinkSqlBusiness的入口函数
     * @param sql
     * @return
     */
    public static FlinkSqlTextBusiness build(String sql){
        FlinkSqlTextBusiness flinkSqlBusiness = new FlinkSqlTextBusiness();
        flinkSqlBusiness.fullSql = sql;
        return flinkSqlBusiness;
    }


    /**
     * 分析原始sql语句
     */
    public void initSql() throws FlinkSqlException {

        //将sql转换为小写
        fullSql = fullSql.toLowerCase();
        //去除注释
        fullSql = fullSql.replaceAll("\\--.*(\n|\r)","");

        //将在一起的空格合并为一个空格
        fullSql = fullSql.replaceAll("\\s+"," ");

        //将带有 __u__ 的转换为大写
        fullSql = dealUpperWord(fullSql);

        //根据分号将sql脚本分解为多个单行sql
        String[] sqlArray = fullSql.split(";");

        for(String sql : sqlArray) {

            sql = sql.trim();
            if(sql != null && sql.length()!= 0) {
                initFlinkComponent(sql);
            }
        }
    }


    /**
     * create the components of flink
     * @param sql
     */
    private void initFlinkComponent(String sql) throws FlinkSqlException {


        if(sql.toLowerCase().startsWith(ConstantForSQL.KEYWORD_CreateInputStream)){
             initCreateInputStreamComponent(sql);
        }
        else if(sql.toLowerCase().startsWith(ConstantForSQL.KEYWORD_CreateOutputStream)){
            initCreateOutputStreamComponent(sql);
        }
        else if(sql.toLowerCase().startsWith(ConstantForSQL.KEYWORD_InsertIntoStream)){
            initInsertIntoComponent(sql);
        }
        else if(sql.toLowerCase().startsWith(ConstantForSQL.KEYWORD_SubmitApplication)){
            initSubmitApplicationComponent(sql);
        }
        else{
            throw new FlinkSqlException("这是一个不支持的sql语句.["+sql+"]");
        }


    }


    /**
     * 处理sql语句 类似:
     *
     CREATE INPUT STREAM s1
     (name  STRING,count INT)
     SOURCE Kafka010Input
     PROPERTIES ("zookeepers" = "127.0.0.1:2181","prefix" = "/meta_pushmq"
     ,"topic" = "push_rtcp","groupId" = "cqlClient");
     * @param sql
     */
    private void initCreateInputStreamComponent(String sql) throws FlinkSqlException {

        //去除命令关键字
        sql = sql.replace(ConstantForSQL.KEYWORD_CreateInputStream,"");

        //获取stream的名字
        //在 第一个字符开始直到 第一个左括号
        String streamName = sql.substring(0,sql.indexOf(ConstantForSQL.LEFT_PARENTHESIS)).trim();

        //将语句根据 SOURCE 分隔为两个部分
        String[] sqlArray = sql.split("\\) source");

        //处理 左边  schema 部分
        String ps = deaParenthesis(sqlArray[0]+")");
        logger.error("##initCreateInputStreamComponent ps:["+ps+"]");

        Map<String,String[]> schemaMap = dealSchema(ps);
        logger.error("##initCreateInputStreamComponent schemaMap:["+ JSONObject.toJSONString(schemaMap)+"]");

        /***
         * 处理右边 来源的属性部分
         */
        //获取来源的类型 ,即 FlexQInput 等
        //内容大写
        String sourceType = sqlArray[1].substring(0,sqlArray[1].indexOf(ConstantForSQL.LEFT_PARENTHESIS)).trim();

        //处理输入的 mq 的属性信息
        String ss = deaParenthesis(sqlArray[1]);
        logger.error("##initCreateInputStreamComponent ss:["+ps+"]");
        Map<String,String> propertiesMap = dealProperties(ss);
        logger.error("##initCreateInputStreamComponent propertiesMap:["+JSONObject.toJSONString(propertiesMap)+"]");

        //Kafka010Input
        if(sourceType.toLowerCase().startsWith("kafka010input")){
             initKafka010InputComponent(streamName,schemaMap,propertiesMap);
        }
        //FlexQInput
        else if(sourceType.toLowerCase().startsWith("flexqinput")){
            initFlexQInputComponent(streamName,schemaMap,propertiesMap);
        }
        else{
            throw new FlinkSqlException("initCreateInputStreamComponent:["+sourceType+"]");
        }
    }



    /**
     * 处理sql语句 类似:
     * CREATE OUTPUT STREAM s2
     (name2 STRING,count2 INT)
     SINK Kafka09Output
     PROPERTIES ("zookeepers" = "172.0.0.1:2181","prefix" = "/meta_pushmq"
     ,"topic" = "push_rtcp_out");
     * @param sql
     */
    private void initCreateOutputStreamComponent(String sql) throws FlinkSqlException {

        //去除命令关键字
        sql = sql.replace(ConstantForSQL.KEYWORD_CreateOutputStream,"");

        //获取stream的名字
        //在 第一个字符开始直到 第一个左括号
        String streamName = sql.substring(0,sql.indexOf(ConstantForSQL.LEFT_PARENTHESIS)).trim();

        //将语句根据 SINK 分隔为两个部分
        String[] sqlArray = sql.split("\\) sink");

        //处理 左边  schema 部分
        Map<String,String[]> schemaMap = dealSchema(deaParenthesis(sqlArray[0]+")"));

        /***
         * 处理右边 来源的属性部分
         */

        //获取来源的类型 ,即 FlexQInput 等
        //内容大写
        String sinkType = sqlArray[1].substring(0,sqlArray[1].indexOf(ConstantForSQL.LEFT_PARENTHESIS)).trim();

        //处理输入的 mq 的属性信息
        //
        Map<String,String> propertiesMap = dealProperties(deaParenthesis(sqlArray[1]));

        //Kafka09Output
        if(sinkType.toLowerCase().startsWith("kafka09output")){
             initKafka09Output(streamName,schemaMap,propertiesMap);
        }
        //FlexQOutput
        else if(sinkType.toLowerCase().startsWith("flexqoutput")){
            initFlexQOutput(streamName,schemaMap,propertiesMap);
        }
        //HBaseOutput
        else if(sinkType.toLowerCase().startsWith("hbaseoutput")){
            initHBaseOutput(streamName,schemaMap,propertiesMap);
        }
        else{
            throw new FlinkSqlException("initCreateOutputStreamComponent:["+sinkType+"]");
        }
    }



    /**
     * 处理sql语句 类似:
     *
     INSERT INTO STREAM s2
     SELECT name as name2,sum(count) as count2  FROM s1 GROUP BY name;
     * @param sql
     */
    private void initInsertIntoComponent(String sql) {

        //去除命令关键字
        sql = sql.replace(ConstantForSQL.KEYWORD_InsertIntoStream,"").trim();

        String name = sql.substring(0,sql.indexOf(" ")).trim();

        String flinkSQL = sql.replace(name,"").trim();

        FlinkTaskLogicComponent flinkTaskLogicComponent = new FlinkTaskLogicComponent(name,flinkSQL);

        flinkComponentList.add(flinkTaskLogicComponent);
    }

    /**
     * 处理sql语句 类似:
     * SUBMIT APPLICATION a2;
     * @param sql
     */
    private void initSubmitApplicationComponent(String sql) {

        String applicationName = sql.replace(ConstantForSQL.KEYWORD_SubmitApplication,"").trim();

        FlinkSubmitTaskComponent flinkSubmitTaskComponent = new FlinkSubmitTaskComponent(applicationName);

        flinkComponentList.add(flinkSubmitTaskComponent);
    }




    /**
     * 初始化kakfa010输入组件
     * @param streamName
     * @param schemaMap
     * @param propertiesMap
     */
    private void initKafka010InputComponent(String streamName, Map<String, String[]> schemaMap, Map<String, String> propertiesMap) throws FlinkSqlException {

        String name = streamName;
        String topic = propertiesMap.get(ConstantForSQL.TOPIC);
        String brokerHostPorts = propertiesMap.get(ConstantForSQL.BROKER);
        String group = propertiesMap.get(ConstantForSQL.GROUPID);
        String[] columnNames = schemaMap.get(ConstantForSQL.COLUMN_NAME);
        TypeInformation[] dataTypeStrArray = convertStringToType(schemaMap.get(ConstantForSQL.DATA_TYPE));

        //source 原始的列名
        String[] columnSourceNames = new String[columnNames.length];
       //sql 里面用的别名
        String[] columnAliasNames = new String[columnNames.length];

        for(int i=0;i<columnNames.length;i++){
            String columnName = columnNames[i];
            String[] columnNameArray = columnName.split(ConstantForSQL.COL_NAME_KEY_SIGN);

            if(columnNameArray.length == 2){
                columnSourceNames[i]=columnNameArray[0];
                columnAliasNames[i]=columnNameArray[1];
            }
            else{
                columnSourceNames[i]=columnNameArray[0];
                columnAliasNames[i]=columnNameArray[0];
            }
        }

        FlinkKafka010InputComponent kafka010InputComponent = new FlinkKafka010InputComponent(name,topic,brokerHostPorts,group,columnSourceNames,columnAliasNames,dataTypeStrArray);

        flinkComponentList.add(kafka010InputComponent);
        flinkComponentMap.put(streamName,kafka010InputComponent);
    }


    /**
     * 初始化 kafka 09 输出组件
     * @param streamName
     * @param schemaMap
     * @param propertiesMap
     */
    private void initKafka09Output(String streamName, Map<String, String[]> schemaMap, Map<String, String> propertiesMap) throws FlinkSqlException {

        String name = streamName;
        String topic = propertiesMap.get(ConstantForSQL.TOPIC);
        String brokerHostPorts = propertiesMap.get(ConstantForSQL.BROKER);
        String[] columnNames = schemaMap.get(ConstantForSQL.COLUMN_NAME);
        TypeInformation[] dataTypeStrArray = convertStringToType(schemaMap.get(ConstantForSQL.DATA_TYPE));

        FlinkKafka09OutputComponent kafka09OutputComponent = new FlinkKafka09OutputComponent(name,topic,brokerHostPorts,columnNames,dataTypeStrArray);

        flinkComponentList.add(kafka09OutputComponent);
        flinkComponentMap.put(streamName,kafka09OutputComponent);
    }


    /**
     * 初始化FlexQ输入组件
     * @param streamName
     * @param schemaMap
     * @param propertiesMap
     */
    private void initFlexQInputComponent(String streamName, Map<String, String[]> schemaMap, Map<String, String> propertiesMap) throws FlinkSqlException {

        String topic = propertiesMap.get(ConstantForSQL.TOPIC);
        String zkHost = propertiesMap.get(ConstantForSQL.ZOOKEEPER);
        String zkPrefix = propertiesMap.get(ConstantForSQL.PREFIX);
        String group = propertiesMap.get(ConstantForSQL.GROUPID);
        String dataFormat = propertiesMap.get(ConstantForSQL.FORMAT);

        if(dataFormat == null){
            dataFormat = ConstantForSQL.FORMAT_NORMAL;
        }


        String[] columnNames = schemaMap.get(ConstantForSQL.COLUMN_NAME);
        TypeInformation[] dataTypeStrArray = convertStringToType(schemaMap.get(ConstantForSQL.DATA_TYPE));

        //source 原始的列名
        String[] columnSourceNames = new String[columnNames.length];
        //sql 里面用的别名
        String[] columnAliasNames = new String[columnNames.length];

        for(int i=0;i<columnNames.length;i++){
            String columnName = columnNames[i];
            String[] columnNameArray = columnName.split(ConstantForSQL.COL_NAME_KEY_SIGN);

            if(columnNameArray.length == 2){
                columnSourceNames[i]=columnNameArray[0];
                columnAliasNames[i]=columnNameArray[1];
            }
            else{
                columnSourceNames[i]=columnNameArray[0];
                columnAliasNames[i]=columnNameArray[0];
            }
        }

        FlinkFlexQInputComponent flinkFlexQInputComponent = new FlinkFlexQInputComponent(streamName,topic,zkHost,zkPrefix,group,dataFormat,columnSourceNames,columnAliasNames,dataTypeStrArray);

        flinkComponentList.add(flinkFlexQInputComponent);
        flinkComponentMap.put(streamName,flinkFlexQInputComponent);
    }

    /**
     * 初始化FlexQ输出组件
     * @param streamName
     * @param schemaMap
     * @param propertiesMap
     */
    private void initFlexQOutput(String streamName, Map<String, String[]> schemaMap, Map<String, String> propertiesMap) throws FlinkSqlException {

        String name = streamName;
        String topic = propertiesMap.get(ConstantForSQL.TOPIC);
        String zkHost = propertiesMap.get(ConstantForSQL.ZOOKEEPER);
        String zkPrefix = propertiesMap.get(ConstantForSQL.PREFIX);

        String[] columnNames = schemaMap.get(ConstantForSQL.COLUMN_NAME);
        TypeInformation[] dataTypeStrArray = convertStringToType(schemaMap.get(ConstantForSQL.DATA_TYPE));

        FlinkFlexQOutputComponent flinkFlexQOutputComponent = new FlinkFlexQOutputComponent(streamName,topic,zkHost,zkPrefix,columnNames,dataTypeStrArray);

        flinkComponentList.add(flinkFlexQOutputComponent);
        flinkComponentMap.put(streamName,flinkFlexQOutputComponent);

    }


    /**
     * 初始化 hbase 输出组件
     * @param streamName
     * @param schemaMap
     * @param propertiesMap
     */
    private void initHBaseOutput(String streamName, Map<String, String[]> schemaMap, Map<String, String> propertiesMap) throws FlinkSqlException {
        String name = streamName;

        String zkHost = propertiesMap.get(HBaseConstant.ZKHOST);
        String zkPort = propertiesMap.get(HBaseConstant.ZKPORT);
        String zkPrefix = propertiesMap.get(HBaseConstant.ZKPREFIX);
        String tableName = propertiesMap.get(HBaseConstant.TABLE_NAME);
        String colFamily = propertiesMap.get(HBaseConstant.COLFAMILY);

        String[] columnNames = schemaMap.get(ConstantForSQL.COLUMN_NAME);
        TypeInformation[] dataTypeStrArray = convertStringToType(schemaMap.get(ConstantForSQL.DATA_TYPE));


        FlinkHBaseOutputComponent flinkHBaseOutputComponent = new FlinkHBaseOutputComponent();
        flinkHBaseOutputComponent.setName(name);
        flinkHBaseOutputComponent.setZkHost(zkHost);
        flinkHBaseOutputComponent.setZkPort(zkPort);
        flinkHBaseOutputComponent.setZkPrefix(zkPrefix);
        flinkHBaseOutputComponent.setTableName(tableName);
        flinkHBaseOutputComponent.setColFamily(colFamily);
        flinkHBaseOutputComponent.setColumnNames(columnNames);
        flinkHBaseOutputComponent.setDataTypeStrArray(dataTypeStrArray);

        flinkComponentList.add(flinkHBaseOutputComponent);
        flinkComponentMap.put(streamName,flinkHBaseOutputComponent);


    }




    //处理schema 格式的信息
    //name  STRING,count INT
    //统一小写
    private static Map<String,String[]> dealSchema(String sql) {

        Map<String,String[]> map = new HashMap<String, String[]>();

        //逗号分隔
        String[] clauseArray =  sql.split(ConstantForSQL.COMMA);

        String[] columnNameArray = new String[clauseArray.length];
        String[] dataTypeArray = new String[clauseArray.length];

        for(int i = 0 ;i < clauseArray.length;i++ ){

            String  clause =  clauseArray[i].trim();

            if(clause.length() != 0){
                String[] kvStrArray = clause.split(" ");



                columnNameArray[i] = kvStrArray[0].trim();
                dataTypeArray[i] = kvStrArray[1].trim();
            }
        }

        map.put(ConstantForSQL.COLUMN_NAME,columnNameArray);
        map.put(ConstantForSQL.DATA_TYPE,dataTypeArray);

        return map;

    }



    //提取 双栝号之间的内容,例如 传入 ("zookeepers" = "172.0.0.1:2181") 返回 "zookeepers" = "172.0.0.1:2181"
    private static String deaParenthesis(String sql) {

        int leftParenthesis = sql.indexOf(ConstantForSQL.LEFT_PARENTHESIS)+1;
        int rightParenthesis = sql.indexOf(ConstantForSQL.RIGHT_PARENTHESIS);

        return sql.substring(leftParenthesis,rightParenthesis);
    }

    //提取 双引号之间的内容,例如 传入 "zk" 返回 zk
    private static String dealQuotation(String s) {

        //将在一起的空格合并为0个空格
        s = s.replaceAll("\\s+","");

        int start = 0;
        int end = s.length();

        if(s.startsWith("\"")){
            start = 1;
        }
        if(s.endsWith("\"")){
            end = end -1;
        }

        return s.substring(start ,end);
    }

    //处理例如这种语句
    //"zookeepers" = "","prefix" = "/meta_pushmq" ,"topic" = "push_rtcp_out"
    //key 都统一存放小写
    private static Map<String,String> dealProperties(String sql){

        Map<String,String> map = new HashMap<String, String>();
        //将在一起的空格合并为0个空格
        sql = sql.replaceAll("\\s+","");
        //逗号分隔
        String[] clauseArray =  sql.split("\",\"");

        for(String clause :  clauseArray){

            clause = clause.trim();
            System.out.println(clause);

            if(clause.length() != 0){
                String[] kvStrArray = clause.split(ConstantForSQL.EQUAL);

                String key = dealQuotation(kvStrArray[0]).toLowerCase();
                String value = dealQuotation(kvStrArray[1]);
                map.put(key,value);
            }

        }
        return map;

    }


    //将字符串数组转换为类型数组
    private static TypeInformation[] convertStringToType(String[] dataTypeStrArray) throws FlinkSqlException {

        TypeInformation[] dataTypeArray = new TypeInformation[dataTypeStrArray.length];


        for(int i = 0;i< dataTypeStrArray.length;i++){

            String type = dataTypeStrArray[i];

            if("string".equals(type)){
                dataTypeArray[i] = Types.STRING();
            }
            else if("short".equals(type)){
                dataTypeArray[i] = Types.SHORT();
            }
            else if("int".equals(type)){
                dataTypeArray[i] = Types.INT();
            }
            else if("long".equals(type)){
                dataTypeArray[i] = Types.LONG();
            }
            else if("date".equals(type)){
                dataTypeArray[i] = Types.SQL_DATE();
            }
            else if("timestamp".equals(type)){
                dataTypeArray[i] = Types.SQL_TIMESTAMP();
            }
            else if("float".equals(type)){
                dataTypeArray[i] = Types.FLOAT();
            }
            else if("double".equals(type)){
                dataTypeArray[i] = Types.DOUBLE();
            }
            else if("byte".equals(type)){
                dataTypeArray[i] = Types.BYTE();
            }
            else{
                throw new FlinkSqlException("类型错误["+dataTypeArray[i]+"]");
            }
        }

        return dataTypeArray;

    }


    /**
     *  因为 列名默认全小写,然后字符串中有的字母为大写 ,
     * 所以 对于 myName 这种json属性设置为 my__u__name ,然后转换为 myName
     * @param fullSql
     * @return
     */
    private static final String TAG_UPPER = "__u__";
    private static String dealUpperWord(String fullSql) {

        String[] attributeNames = fullSql.split(TAG_UPPER);

        String result = attributeNames[0];

        for(int i = 1;i<attributeNames.length;i++){

            char[] cs=attributeNames[i].toCharArray();
            cs[0]= String.valueOf(cs[0]).toUpperCase().toCharArray()[0];

            result += String.valueOf(cs);
        }

        return result;

    }



    public List<FlinkComponent> getFlinkComponentList() {
        return flinkComponentList;
    }

    public Map<String, FlinkComponent> getFlinkComponentMap() {
        return flinkComponentMap;
    }

}
