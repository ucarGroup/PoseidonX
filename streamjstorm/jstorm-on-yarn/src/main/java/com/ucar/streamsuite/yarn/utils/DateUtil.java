package com.ucar.streamsuite.yarn.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

    public static  String getTodayString(){
        SimpleDateFormat Y_M_D_FORMAT = new SimpleDateFormat("yyyyMMdd");
        return Y_M_D_FORMAT.format(new Date());
    }

    public static String getNowSecondTime(){
        SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
        return format.format(new Date());
    }
}
