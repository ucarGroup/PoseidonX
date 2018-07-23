package com.ucar.streamsuite.common.util;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * 处理异常的工具类
 * Created on 2017/1/18 下午5:29:14
 */
public class ExceptionUtils {

    /**
     * 获取异常的堆栈信息
     *
     * @param t
     * @return
     */
    public  static String getStackTrace(Throwable t)
    {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw);
        try
        {
            t.printStackTrace(pw);
            return sw.toString();
        }
        finally
        {
            pw.close();
        }
    }
}
