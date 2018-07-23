package com.ucar.streamsuite.moniter.business;

import java.util.Set;

/**
 * Description: 告警实现类 用户可以自行实现告警方式。内容会根据系统埋点传入
 * Created on 2018/1/30 上午9:18
 *
 *
 */
public interface AlarmBusiness {

     void sendEmail( Set<String> mails,String title,String content);

     void sendPhone(Set<String> phones,String content);
}
