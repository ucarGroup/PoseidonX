package com.ucar.flinkcomponent.connectors.hbase;

import java.nio.charset.Charset;

public class HBaseConstant {

    public static final  String ZKHOST = "zookeepers";
    public static final  String ZKPORT = "port";
    public static final  String ZKPREFIX = "prefix";
    public static final  String TABLE_NAME = "tablename"; //表名
    public static final  String COLFAMILY = "colfamily"; //列族

    public static final Charset UTF8 = Charset.forName("UTF-8");


}
