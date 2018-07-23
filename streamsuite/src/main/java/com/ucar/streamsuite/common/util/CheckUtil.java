package com.ucar.streamsuite.common.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Description: 校验工具类
 * Created on 2018/1/31 下午3:25
 *
 */
public class CheckUtil {

    /**
     * 邮箱验证
     * @param email
     * @return
     */
    public static boolean checkEmail(String email)
    {
        boolean flag = false;
        try {
            String check = "^([a-z0-9A-Z]+[-|_|\\.]?)+[a-z0-9A-Z]@([a-z0-9A-Z]+(-[a-z0-9A-Z]+)?\\.)+[a-zA-Z]{2,}$";
            Pattern regex = Pattern.compile(check);
            Matcher matcher = regex.matcher(email);
            flag = matcher.matches();
        } catch (Exception e) {
            flag = false;
        }
        return flag;
    }

    /**
     * 手机号验证
     * @param  str
     * @return 验证通过返回true
     */
    public static boolean checkMobile(final String str) {
        Pattern p = null;
        Matcher m = null;
        boolean b = false;
        p = Pattern.compile("^[1][3,4,5,7,8][0-9]{9}$"); // 验证手机号
        m = p.matcher(str);
        b = m.matches();
        return b;
    }
}
