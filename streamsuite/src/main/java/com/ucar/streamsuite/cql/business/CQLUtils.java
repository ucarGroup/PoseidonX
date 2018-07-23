package com.ucar.streamsuite.cql.business;


import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

/**
 * Created on 2017/1/23 下午5:46:25
 *
 *
 */
public class CQLUtils {
    private static final Logger LOG = LoggerFactory.getLogger(CQLUtils.class);
    private static final String CQL_LINE_SEPARATOR = ";[;\t ]*\n";

    public static List<String> analyzeContent(String cqlContent){

        List<String> cqlList = null;

        StringBuilder cqlBuffer = new StringBuilder();

        cqlContent = cqlContent.replaceAll("\r\n","\n");

        String[] cqlTempArray = cqlContent.split("\n");

        for(String cqlTempLine:cqlTempArray){

            if (isEmpty(cqlTempLine))
            {
                continue;
            }

            if (isCommentsLine(cqlTempLine.trim()))
            {
                LOG.debug("throw comments line {}", cqlTempLine);
                continue;
            }
            cqlBuffer.append(cqlTempLine).append("\n");
        }

        LOG.debug("cqlBuffer {}", cqlBuffer.toString());

        cqlList = getCQLResult(cqlBuffer);

        return cqlList;
    }
    /**
     * 获取解析好的CQL语句
     */
    public static List< String > getCQLResult(StringBuilder cqlBuffer)
    {
        List< String > results = Lists.newArrayList();
        String[] cqls = replaceBlank(cqlBuffer.toString()).split(CQL_LINE_SEPARATOR);
        for (String cql : cqls)
        {
            if (isEmpty(cql))
            {
                continue;
            }

            results.add(cql);
        }

        return results;
    }


    /**
     * 字符串是否为空
     *
     */
    public static boolean isEmpty(String str)
    {
        if (Strings.isNullOrEmpty(str))
        {
            return true;
        }

        if (Strings.isNullOrEmpty(str.trim()))
        {
            return true;
        }

        return false;
    }

    /**
     * 是否是注释行
     */
    public static boolean isCommentsLine(String newLine)
    {
        return newLine.startsWith("--") || newLine.startsWith("/*") || newLine.startsWith("*");
    }

    /**
     * 替换字符串中的空格等特殊符号
     */
    public static String replaceBlank(String str)
    {
        Pattern pattern = Pattern.compile("\t|\r");
        String dest = "";
        if (str != null)
        {
            Matcher m = pattern.matcher(str);
            dest = m.replaceAll(" ");
        }
        return dest;
    }

}