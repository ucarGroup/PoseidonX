package com.ucar.streamsuite.common.util;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.HttpVersion;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.client.DefaultHttpRequestRetryHandler;
import org.apache.http.impl.conn.tsccm.ThreadSafeClientConnManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.apache.log4j.Logger;

/**
 * HTTPCLIENT 封装类
 * Created on 2018/1/30 上午10:59
 *
 */
public final class HttpSendClientFactory {

    private static final Logger logger = Logger.getLogger(HttpSendClientFactory.class);

    public static final int MAX_TOTAL_CONNECTIONS_DEFAULT = 512;
    private int maxTotalConnections = MAX_TOTAL_CONNECTIONS_DEFAULT;

    public static final int MAX_PER_ROUTE_DEFAULT = 128;
    private int defaultMaxPerRoute = MAX_PER_ROUTE_DEFAULT;

    public static final int CONNECTION_TIMEOUT_DEFAULT = 2000;
    private int connectionTimeOut = CONNECTION_TIMEOUT_DEFAULT;

    public static final int SO_TIMEOUT_DEFAULT = 3000;
    private int soTimeOut = SO_TIMEOUT_DEFAULT;

    public static final int SOCKET_BUFFER_SIZE_DEFAULT = 8192;
    private int socketBufferSize = SOCKET_BUFFER_SIZE_DEFAULT;

    public static final int RETRY_COUNT_DEFAULT = 3;
    private int retryCount = RETRY_COUNT_DEFAULT;

    /**
     * 永不超时 设置的值
     */
    private static final int SO_TIMEOUT_INFINITE = 0;

    private static volatile HttpSendClient instance = null;
    private static volatile HttpSendClient infiniteInstance = null;
    private static byte[] lock = new byte[0];
    private static byte[] infiniteLock = new byte[0];

    /**
     * 请求会超时
     * <p>
     * 单例
     * <p>
     * 默认连接超时时间：2秒<br>
     * 默认执行超时时间：3秒<br>
     * 
     * @return
     */
    public static HttpSendClient getInstance() {
        if (instance == null) {
            synchronized (lock) {
                if (instance == null) {
                    instance = httpSendClient(SO_TIMEOUT_DEFAULT, CONNECTION_TIMEOUT_DEFAULT);
                    logger.info("HttpSendClientFactory New HttpSendClient Instance");
                }
            }
        }
        return instance;
    }

    /**
     * 请求不超时
     * <p>
     * 单例
     * <p>
     * 默认连接超时时间：2秒<br>
     * 
     * @return
     */
    public static HttpSendClient getNTOInstance() {
        if (infiniteInstance == null) {
            synchronized (infiniteLock) {
                if (infiniteInstance == null) {
                    infiniteInstance = httpSendClient(SO_TIMEOUT_INFINITE, CONNECTION_TIMEOUT_DEFAULT);
                    logger.info("HttpSendClientFactory New Not TimeOut HttpSendClient Instance");
                }
            }
        }
        return infiniteInstance;
    }

    private static HttpSendClient httpSendClient(int readTimeout, int connectionTimeout) {
        HttpSendClientFactory factory = new HttpSendClientFactory();
        factory.setSoTimeOut(readTimeout);
        factory.setConnectionTimeOut(connectionTimeout);
        return factory.newHttpSendClient();
    }

    /**
     * 创建一个{@link HttpSendClient} 实例
     * <p>
     * 多态
     * 
     * @return
     */
    public HttpSendClient newHttpSendClient() {
        HttpParams params = new BasicHttpParams();
        HttpConnectionParams.setConnectionTimeout(params, getConnectionTimeOut());
        HttpConnectionParams.setSoTimeout(params, getSoTimeOut());
        // HttpConnectionParams.setLinger(params, 1);

        HttpProtocolParams.setVersion(params, HttpVersion.HTTP_1_1);
        HttpProtocolParams.setContentCharset(params, HTTP.UTF_8);
        // 解释: 握手的目的，是为了允许客户端在发送请求内容之前，判断源服务器是否愿意接受请求（基于请求头部）。
        // Expect:100-Continue握手需谨慎使用，因为遇到不支持HTTP/1.1协议的服务器或者代理时会引起问题。
        // 默认开启
        // HttpProtocolParams.setUseExpectContinue(params, false);
        HttpConnectionParams.setTcpNoDelay(params, true);
        HttpConnectionParams.setSocketBufferSize(params, getSocketBufferSize());

        ThreadSafeClientConnManager threadSafeClientConnManager = new ThreadSafeClientConnManager();
        threadSafeClientConnManager.setMaxTotal(getMaxTotalConnections());
        threadSafeClientConnManager.setDefaultMaxPerRoute(getDefaultMaxPerRoute());

        DefaultHttpRequestRetryHandler retryHandler = new DefaultHttpRequestRetryHandler(getRetryCount(), false);

        DefaultHttpClient httpClient = new DefaultHttpClient(threadSafeClientConnManager, params);
        httpClient.setHttpRequestRetryHandler(retryHandler);
        return new HttpSendClient(httpClient);
    }

    public int getDefaultMaxPerRoute() {
        return defaultMaxPerRoute;
    }

    /**
     * 设置每个路由链接数默认值 default 32
     * 
     * @param int
     */
    public void setDefaultMaxPerRoute(int defaultMaxPerRoute) {
        this.defaultMaxPerRoute = defaultMaxPerRoute;
    }

    public int getConnectionTimeOut() {
        return connectionTimeOut;
    }

    /**
     * 设置连接超时时间 default 2000
     * 
     * @param int
     */
    public void setConnectionTimeOut(int connectionTimeOut) {
        this.connectionTimeOut = connectionTimeOut;
    }

    public int getSoTimeOut() {
        return soTimeOut;
    }

    /**
     * 设置请求超时时间 default 3000
     * 
     * @param int
     */
    public void setSoTimeOut(int soTimeOut) {
        this.soTimeOut = soTimeOut;
    }

    public int getSocketBufferSize() {
        return socketBufferSize;
    }

    /**
     * 设置socket buffer size default 8192
     * 
     * @param int
     */
    public void setSocketBufferSize(int socketBufferSize) {
        this.socketBufferSize = socketBufferSize;
    }

    public int getRetryCount() {
        return retryCount;
    }

    /**
     * 设置请求失败重试次数 default 3
     * 
     * @param int
     */
    public void setRetryCount(int retryCount) {
        this.retryCount = retryCount;
    }

    public int getMaxTotalConnections() {
        return maxTotalConnections;
    }

    /**
     * 设置最大链接数 default 128
     * 
     * @param int
     */
    public void setMaxTotalConnections(int maxTotalConnections) {
        this.maxTotalConnections = maxTotalConnections;
    }
}
