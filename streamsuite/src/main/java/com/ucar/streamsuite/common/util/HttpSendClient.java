package com.ucar.streamsuite.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.ParseException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.DeflateDecompressingEntity;
import org.apache.http.client.entity.GzipDecompressingEntity;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ByteArrayEntity;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;


/**
 * HTTPCLIENT 封装类
 * Created on 2018/1/30 上午10:59
 *
 */
public class HttpSendClient {

    private HttpClient httpClient;

    private String agent =  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/51.0.2704.79 Chrome/51.0.2704.79 Safari/537.36";

    private Class<?>[] paramTypes = { String.class, Map.class };

    public HttpSendClient(HttpClient httpClient) {
        this.httpClient = httpClient;
    }

    /**
     * Post 方式请求 ，并返回HTTP_STATUS_CODE码
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @return int
     * @throws ClientProtocolException
     * @throws IOException
     */
    public int postReturnHttpCode(String address, Map<String, Object> params) throws ClientProtocolException,
            IOException {
        HttpPost httpPost = new HttpPost(address);
        HttpResponse httpResponse = null;
        try {
            List<NameValuePair> data = buildPostData(params);
            httpPost.setEntity(new UrlEncodedFormEntity(data, HTTP.UTF_8));
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
            httpPost.setHeader("User-Agent", agent);
            httpResponse = httpClient.execute(httpPost);
            return httpResponse.getStatusLine().getStatusCode();
        } finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity()); //会自动释放连接
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * Get 方式请求 ，并返回HTTP_STATUS_CODE码
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @return int
     * @throws ClientProtocolException
     * @throws IOException
     */
    public int getReturnHttpCode(String address, Map<String, Object> params) throws ClientProtocolException,
            IOException {

        String paramsStr = buildGetData(params);
        HttpResponse httpResponse = null;
        HttpGet httpGet = new HttpGet(address + paramsStr);
        try {
            httpGet.setHeader("User-Agent", agent);
            httpResponse = httpClient.execute(httpGet);
            return httpResponse.getStatusLine().getStatusCode();
        } finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity()); //会自动释放连接
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * 回调接口
     */
    public static interface CallBack<T> {
        /**
         * 回调方法
         * 
         * @param String response 响应内容
         * @return T 转换的对象，自己封装
         */
        public T call(String response);

    }

    /**
     * Post 方式请求 ，并执行回调接口，返回 T 对象
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @param CallBack<T> callback 回调接口
     * @param T
     * @throws ClientProtocolException
     * @throws IOException
     */
    public <T> T post(String address, Map<String, Object> params, CallBack<T> callback) throws ClientProtocolException,
            IOException {
        String ret = post(address, params);
        return callback.call(ret);
    }

    /**
     * Get 方式请求 ，并执行回调接口，返回 T 对象
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @param CallBack<T> callback 回调接口
     * @param T
     * @throws ClientProtocolException
     * @throws IOException
     */
    public <T> T get(String address, Map<String, Object> params, CallBack<T> callback) throws ClientProtocolException,
            IOException {
        String ret = get(address, params);
        return callback.call(ret);
    }

    /**
     * Post 方式请求，返回响应内容
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @return String 响应内容
     * @throws ClientProtocolException
     * @throws IOException
     */
    public String post(String address, Map<String, Object> params) throws ClientProtocolException, IOException {

        HttpPost httpPost = new HttpPost(address);
        HttpResponse httpResponse = null;
        try {
            List<NameValuePair> data = buildPostData(params);
            httpPost.setEntity(new UrlEncodedFormEntity(data, HTTP.UTF_8));
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
            httpPost.setHeader("User-Agent", agent);

            httpResponse = httpClient.execute(httpPost);

            HttpEntity entity = httpResponse.getEntity();
            return EntityUtils.toString(entity, HTTP.UTF_8);
        } finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity()); //会自动释放连接
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * Get 方式请求，返回响应内容
     *
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @return String 响应内容
     * @throws ClientProtocolException
     * @throws IOException
     */
    public String get(String address, Map<String, Object> params) throws ClientProtocolException, IOException {

        String paramsStr = buildGetData(params);
        HttpResponse httpResponse = null;
        HttpGet httpGet = new HttpGet(address + paramsStr);
        try {
            httpGet.setHeader("User-Agent", agent);
            httpResponse = httpClient.execute(httpGet);
            HttpEntity entity = httpResponse.getEntity();
            return EntityUtils.toString(entity, HTTP.UTF_8);
        } finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity()); //会自动释放连接
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * Post 压缩发送请求，解压缩接收返回数据. If both gzip and deflate compression will be
     * accepted in the HTTP response. please choose the method
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @return String 响应内容
     * @throws ClientProtocolException
     * @throws IOException
     */
    public String postWithCompression(String address, Map<String, Object> params) throws ClientProtocolException,
            IOException {
        HttpPost httpPost = new HttpPost(address);
        HttpResponse httpResponse = null;
        try {
            List<NameValuePair> data = buildPostData(params);
            httpPost.setEntity(new UrlEncodedFormEntity(data, HTTP.UTF_8));
            httpPost.setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
            httpPost.setHeader("User-Agent", agent);
            httpPost.setHeader("Accept-Encoding", "gzip,deflate");

            httpResponse = httpClient.execute(httpPost);

            return uncompress(httpResponse);
        } finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity()); //会自动释放连接
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * Get 压缩发送请求，解压缩接收返回数据. If both gzip and deflate compression will be
     * accepted in the HTTP response. please choose the method
     * 
     * @param String address 请求地址
     * @param Map<String, Object> params 请求参数
     * @return String 响应内容
     * @throws ClientProtocolException
     * @throws IOException
     */
    public String getWithCompression(String address, Map<String, Object> params) throws ClientProtocolException,
            IOException {

        String paramsStr = buildGetData(params);
        HttpGet httpGet = new HttpGet(address + paramsStr);
        HttpResponse httpResponse = null;
        try {
            httpGet.setHeader("User-Agent", agent);
            httpGet.setHeader("Accept-Encoding", "gzip,deflate");

            httpResponse = httpClient.execute(httpGet);
            return uncompress(httpResponse);
        }finally {
            if (httpResponse != null) {
                try {
                    EntityUtils.consume(httpResponse.getEntity()); //会自动释放连接
                } catch (Exception e) {
                }
            }
        }
    }

    /**
     * 验证HTTP_STAUSE_CODE 是否成功
     * 
     * @param int
     * @return boolean
     * 
     */
    public boolean isOK(int code) {
        return code == HttpStatus.SC_OK;
    }

    /**
     * 关闭HTTPCLIENT
     */
    public synchronized void shutdown() {
        httpClient.getConnectionManager().shutdown();
    }

    private List<NameValuePair> buildPostData(Map<String, Object> params) {
        if (params == null || params.size() == 0) {
            return new ArrayList<NameValuePair>(0);
        }
        List<NameValuePair> ret = new ArrayList<NameValuePair>(params.size());
        for (String key : params.keySet()) {
            Object p = params.get(key);
            if (key != null && p != null) {
                NameValuePair np = new BasicNameValuePair(key, p.toString());
                ret.add(np);
            }
        }
        return ret;
    }

    private String buildGetData(Map<String, Object> params) {
        StringBuilder builder = new StringBuilder();
        if (params != null && params.size() != 0) {
            for (Map.Entry<String, Object> entry : params.entrySet()) {
                String key = entry.getKey();
                Object value = entry.getValue();
                if (key == null || key.trim().length() == 0 || value == null) {
                    continue;
                }
                if (builder.length() > 0) {
                    builder.append("&");
                } else {
                    builder.append("?");
                }
                builder.append(key).append("=").append(value);
            }
        }
        return builder.toString();
    }

    private String uncompress(HttpResponse httpResponse) throws ParseException, IOException {
        int ret = httpResponse.getStatusLine().getStatusCode();
        if (!isOK(ret)) {
            return null;
        }

        // Read the contents
        String respBody = null;
        HttpEntity entity = httpResponse.getEntity();
        String charset = EntityUtils.getContentCharSet(entity);
        if (charset == null) {
            charset = "UTF-8";
        }

        // "Content-Encoding"
        Header contentEncodingHeader = entity.getContentEncoding();
        if (contentEncodingHeader != null) {
            String contentEncoding = contentEncodingHeader.getValue();
            if (contentEncoding.contains("gzip")) {
                respBody = EntityUtils.toString(new GzipDecompressingEntity(entity), charset);
            } else if (contentEncoding.contains("deflate")) {
                respBody = EntityUtils.toString(new DeflateDecompressingEntity(entity), charset);
            }
        } else {
            // "Content-Type"
            Header contentTypeHeader = entity.getContentType();
            if (contentTypeHeader != null) {
                String contentType = contentTypeHeader.getValue();
                if (contentType != null) {
                    if (contentType.startsWith("application/x-gzip-compressed")) {
                        respBody = EntityUtils.toString(new GzipDecompressingEntity(entity), charset);
                    } else if (contentType.startsWith("application/x-deflate")) {
                        respBody = EntityUtils.toString(new DeflateDecompressingEntity(entity), charset);
                    }
                }
            }
        }
        return respBody;
    }

}
