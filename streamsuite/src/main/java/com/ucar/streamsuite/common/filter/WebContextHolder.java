package com.ucar.streamsuite.common.filter;
  
import com.ucar.streamsuite.common.constant.StreamContant;

import com.ucar.streamsuite.user.po.UserPO;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;  
import javax.servlet.http.HttpSession;

/**
 * Description: threadLocal中获得 HttpServletRequest HttpServletResponse，和用户dto信息
 * Created on 2018/1/18 下午4:33
 *
 */
public class WebContextHolder {

    private static ThreadLocal<HttpServletRequest> requestLocal = new ThreadLocal<HttpServletRequest>();
    private static ThreadLocal<HttpServletResponse> responseLocal = new ThreadLocal<HttpServletResponse>();

    protected static void removeRequest() {
        requestLocal.remove();
    }

    protected static void removeResponse() {
        responseLocal.remove();
    }

    protected static void setRequest(HttpServletRequest request) {
        if (requestLocal == null)
            requestLocal = new ThreadLocal();
        requestLocal.set(request);
    }

    protected static void setResponse(HttpServletResponse response) {
        if (responseLocal == null)
            responseLocal = new ThreadLocal();
        responseLocal.set(response);
    }

    public static HttpServletResponse getResponse() {
        if (responseLocal == null)
            return null;
        else
            return (HttpServletResponse) responseLocal.get();
    }

    public static HttpServletRequest getRequest() {  
        if (requestLocal == null)  
            return null;  
        else  
            return (HttpServletRequest) requestLocal.get();  
    }

    public static HttpSession getSession() {
        if (requestLocal == null)
            return null;
        if (requestLocal.get() == null)
            return null;
        else
            return ((HttpServletRequest) requestLocal.get()).getSession();
    }

    public static String getContextPath() {  
        if (getRequest() == null)  
            return null;  
        else  
            return (new StringBuilder()).append(getRequest().getContextPath()).append("/").toString();  
    }  
  
    public static String getCurrRequestURI() {  
        if (getRequest() == null)  
            return null;  
        else  
            return (new StringBuilder()).append(getRequest().getRequestURI().replace(getRequest().getContextPath(), ""))  
                    .append("/").toString();  
    }  

    public static UserPO getLoginUser() {
        if (getSession() == null)
            return null;
        Object obj = getSession().getAttribute(StreamContant.SESSION_USER);
        if (obj == null)
            return null;
        else
            return (UserPO) obj;
    }

}  