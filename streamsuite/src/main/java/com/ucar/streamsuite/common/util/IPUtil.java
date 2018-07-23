package com.ucar.streamsuite.common.util;

import javax.servlet.http.HttpServletRequest;

public class IPUtil {

    /**
     * 获取请求ip
     * <p>
     * 逻辑：<br>
     * 1. 先从header X-Forwarded-For 里获取逗号隔开的第1位ip<br>
     * 2. 判断是否私有ip，如果不是就返回<br>
     * 3. 如果是私有ip，再从header X-Real-IP 里获取，如果存在就返回<br>
     * 4. 如果不存在，再从remoteAddr获取<br>
     * <p>
     * 如果不对打debug日志<br>
     * 
     * @param request
     * @return
     */
    public static String getIpAddr(final HttpServletRequest request) {
        // CDN地址
        String ip = request.getHeader("Cdn-Src-Ip");
        if (ip != null && ip.trim().length() != 0) {
            return ip.trim();
        }

        ip = request.getHeader("X-Forwarded-For");
        if (ip != null && ip.indexOf(',') > 0) {
            String[] tmp = ip.split("[,]");
            for (int i = 0; tmp != null && i < tmp.length; i++) {
                if (tmp[i] != null && tmp[i].length() > 0 && !"unknown".equalsIgnoreCase(tmp[i])) {
                    ip = tmp[i].trim();
                    break;
                }
            }
        }
        if (!isUnkown(ip)) {
            if (isPrivateIp(ip)) {
                ip = request.getHeader("X-Real-IP");
            }
        }

        if (isUnkown(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (isUnkown(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (isUnkown(ip)) {
            ip = request.getRemoteAddr();
        }
        return (ip != null) ? ip.trim() : null;
    }

    private static boolean isUnkown(String ip) {
        if (ip == null || ip.trim().length() == 0 || "unknown".equalsIgnoreCase(ip.trim())) {
            return true;
        }
        return false;
    }

    private static final int[][] PRIVATE_IPA = new int[][] { { 10, 0, 0, 0 }, { 10, 255, 255, 255 } };
    private static final int[][] PRIVATE_IPB = new int[][] { { 172, 16, 0, 0 }, { 172, 31, 255, 255 } };
    private static final int[][] PRIVATE_IPC = new int[][] { { 192, 168, 0, 0 }, { 192, 168, 255, 255 } };

    /**
     * 私有IP地址范围：<br>
     * A: 10.0.0.0~10.255.255.255 即10.0.0.0/8<br>
     * B: 172.16.0.0~172.31.255.255即172.16.0.0/12<br>
     * C: 192.168.0.0~192.168.255.255 即192.168.0.0/16<br>
     * 
     * @param ip
     * @return
     */
    public static boolean isPrivateIp(String ip) {
        String[] array = ip.split("\\.");
        // ipv6返回false
        if (array.length != 4) {
            return false;
        }
        int[] actual = new int[4];
        for (int i = 0; i < array.length; i++) {
            actual[i] = Integer.parseInt(array[i]);
        }
        return contain(PRIVATE_IPA, actual) || contain(PRIVATE_IPB, actual) || contain(PRIVATE_IPC, actual);
    }

    private static boolean contain(int[][] expect, int[] actual) {
        int[] start = expect[0];
        int[] end = expect[1];
        for (int i = 0; i < actual.length; i++) {
            if (actual[i] < start[i] || actual[i] > end[i]) {
                return false;
            }
        }
        return true;
    }

}
