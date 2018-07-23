package com.ucar.streamsuite.common.util;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.util.Enumeration;

/**
 * Description: 网络工具类
 * Created on 2018/1/18 下午4:33
 *
 */
public class NetUtil {

    /**
     * 获得本机地址
     * @return
     */
    public static String getLocalHostAddress()  {
        InetAddress inetAddress = getLocalHostLANAddress();
        if(inetAddress == null){
            return "";
        }
        return inetAddress.getHostAddress().toString();
    }

    /**
     * 根据域名获得地址
     * @param domain
     * @return
     */
    public static String getHostFromDomain(String domain) {
        try {
            InetAddress address = InetAddress.getByName(domain);
            return address.getHostAddress().toString();
        } catch (Exception e) {
        }
        return "";
    }

    private static InetAddress getLocalHostLANAddress()  {
        InetAddress candidateAddress = null;
        try {
            // 遍历所有的网络接口
            for (Enumeration ifaces = NetworkInterface.getNetworkInterfaces(); ifaces.hasMoreElements(); ) {
                NetworkInterface iface = (NetworkInterface) ifaces.nextElement();
                // 在所有的接口下再遍历IP
                for (Enumeration inetAddrs = iface.getInetAddresses(); inetAddrs.hasMoreElements(); ) {
                    InetAddress inetAddr = (InetAddress) inetAddrs.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {// 排除loopback类型地址
                        if (inetAddr.isSiteLocalAddress()) {
                            // 如果是site-local地址，就是它了
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            // site-local类型的地址未被发现，先记录候选地址
                            candidateAddress = inetAddr;
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                return candidateAddress;
            }
            // 如果没有发现 non-loopback地址.只能用最次选的方案
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            return jdkSuppliedAddress;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

}
