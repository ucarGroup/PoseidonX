package com.ucar.streamsuite.common.filter;

import com.ucar.streamsuite.common.constant.StreamContant;

import com.ucar.streamsuite.dao.mysql.UserDao;
import com.ucar.streamsuite.user.po.UserPO;
import org.apache.commons.lang.StringUtils;

import org.springframework.boot.web.servlet.ServletComponentScan;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import javax.servlet.*;
import javax.servlet.annotation.WebFilter;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;

/**
 * Description: WebContextFilter   Request Response 放入threadLocal中
 * Created on 2018/1/18 下午4:33
 *
 */
@Component
@ServletComponentScan
@WebFilter(urlPatterns = "/*",filterName = "webContextFilter")
public class WebContextFilter implements Filter {

    @Resource
    private UserDao userDao;

    public void destroy() {
    }

    public void doFilter(ServletRequest request, ServletResponse response,
                         FilterChain chain) throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest)request;
        HttpServletResponse httpResponse = (HttpServletResponse)response;
        WebContextHolder.setRequest(httpRequest);
        WebContextHolder.setResponse(httpResponse);

        try{
            //请求的路径
            Boolean checkResult;
            String contextPath=httpRequest.getContextPath();
            String url = httpRequest.getRequestURL().toString();

            if(url.contains((contextPath + "/#/login"))
                    || url.contains((contextPath + "/user/login"))
                    || url.endsWith(contextPath+"/")
                    || url.endsWith(".js")
                    || url.endsWith(".css")
                    || url.endsWith(".jpg")
                    || url.endsWith(".png")
                    || url.endsWith(".gif")
                    || url.endsWith(".svg")
                    || url.contains((contextPath + "/remote/"))
                    || url.contains((contextPath + "/healthCheck.jsp"))){
                chain.doFilter(request, response);
            }else{
                checkResult = checkByCookie(httpRequest);
                if (!checkResult) {
                    httpResponse.sendRedirect(contextPath + "/#/login");
                }else{
                    chain.doFilter(request, response);
                }
            }
        }finally{
            WebContextHolder.removeRequest();
            WebContextHolder.removeResponse();
        }
    }

    public void init(FilterConfig filterConfig) throws ServletException {
    }

    private boolean checkByCookie(HttpServletRequest request) {
        boolean checkResult = false;
        String userName = getCommonUserInfo(request);

        if(StringUtils.isNotBlank(userName)){
            Object obj = request.getSession().getAttribute(StreamContant.SESSION_USER);
            if (obj == null){
                UserPO userPO = userDao.getUserByName(userName);
                if(userPO != null){
                    request.getSession().setAttribute(StreamContant.SESSION_USER,userPO);
                }else{
                    return false;
                }
            }
            checkResult = true;
        }
        return checkResult;
    }

    private String getCommonUserInfo(HttpServletRequest request) {
        String loginInfo = "";
        if(request.getCookies() != null) {
            for (Cookie cookie : request.getCookies()) {
                if (StreamContant.SESSION_USER.equals(cookie.getName())) {
                    loginInfo = cookie.getValue();
                    break;
                }
            }
        }
        return loginInfo;
    }
}  