package com.ucar.streamsuite.moniter.business.impl;

import com.ucar.streamsuite.moniter.business.AlarmBusiness;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.CoreConnectionPNames;
import org.apache.http.params.HttpParams;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Set;

public class AlarmBusinessImpl implements AlarmBusiness {

    private final static Logger LOGGER = LoggerFactory.getLogger(AlarmBusinessImpl.class);

    @Override
    public void sendEmail(Set<String> mails,String title,String content) {
    }

    @Override
    public void sendPhone(Set<String> phones,String content) {
    }
}
