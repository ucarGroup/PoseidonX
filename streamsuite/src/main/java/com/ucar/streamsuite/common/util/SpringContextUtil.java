package com.ucar.streamsuite.common.util;


import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

/**
 * 此类可以取得Spring的上下文.
 * Created on 2018/1/18 下午4:33
 *
 */
@Service
public class SpringContextUtil implements ApplicationContextAware {

    protected static ApplicationContext context;
    
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        context = applicationContext;
    }

    public static ApplicationContext getContext() {
        return context;
    }
}
