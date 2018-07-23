package com.ucar.streamsuite.common.util;

import sun.misc.BASE64Decoder;

import java.io.UnsupportedEncodingException;

/**
 * Description: Base64编码工具类
 * Created on 2018/1/18 下午4:33
 *
 */
public class Base64Util {

    /**
     * 将 s 进行 BASE64 编码
     *
     * @return String
     * @author lifq
     * @date 2015-3-4 上午09:24:02
     */
    public static String encode(String s) {
        if (s == null)
            return null;
        String res = "";
        try {
            res = new sun.misc.BASE64Encoder().encode(s.getBytes("UTF-8"));
            res = res.replace("\r","").replace("\n","").replace(" ","");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return res;
    }

    public static String decode(String s) {
        if (s == null)
            return null;
        BASE64Decoder decoder = new BASE64Decoder();
        try {
            byte[] b = decoder.decodeBuffer(s);
            return new String(b,"UTF-8");
        } catch (Exception e) {
            return null;
        }
    }

}
