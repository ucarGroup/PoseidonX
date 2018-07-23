package com.ucar.streamsuite.common.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.codec.binary.Base64;

/**
 * Gzip压缩类
 * 
 * @author yuanxl
 * 
 */
public class GzipUtil {

    /**
     * GZIP 压缩字符串（utf-8）,并且BASE64编码
     * 
     * @param String str
     * @return String
     * @throws IOException
     */
    public static String compressString(String str) throws IOException {
        return compressString(str, "UTF-8");
    }

    /**
     * GZIP 压缩字符串,并且BASE64编码
     * 
     * @param String str
     * @param String charsetName
     * @return String
     * @throws IOException
     */
    public static String compressString(String str, String charsetName) throws IOException {
        if (str == null || str.trim().length() == 0) {
            return str;
        }
        return Base64.encodeBase64String(compressString2byte(str, charsetName));
    }

    /**
     * GZIP 压缩字符串（utf-8）
     * 
     * @param String str
     * @param String charsetName
     * @return byte[]
     * @throws IOException
     */
    public static byte[] compressString2byte(String str) throws IOException {
        return compressString2byte(str, "UTF-8");
    }

    /**
     * GZIP 压缩字符串
     * 
     * @param String str
     * @param String charsetName
     * @return byte[]
     * @throws IOException
     */
    public static byte[] compressString2byte(String str, String charsetName) throws IOException {
        if (str == null || str.trim().length() == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        gzip.write(str.getBytes(charsetName));
        gzip.close();
        return out.toByteArray();
    }

    /**
     * GZIP 压缩字节流（utf-8）
     * 
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static byte[] compressInputStream(InputStream inputStream) throws IOException {
        return compressInputStream(inputStream, "UTF-8");
    }

    /**
     * GZIP 压缩字节流
     * 
     * @param inputStream
     * @param charsetName
     * @return
     * @throws IOException
     */
    public static byte[] compressInputStream(InputStream inputStream, String charsetName) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = new GZIPOutputStream(out);
        byte[] buffer = new byte[1024];
        int offset = -1;
        while ((offset = inputStream.read(buffer)) != -1) {
            gzip.write(buffer, 0, offset);
        }
        gzip.close();
        return out.toByteArray();
    }

    /**
     * GZIP 压缩字符串（utf-8）,并且BASE64编码
     * 
     * @param inputStream
     * @return
     * @throws IOException
     */
    public static String compressInputStream2String(InputStream inputStream) throws IOException {
        return compressInputStream2String(inputStream, "UTF-8");
    }

    /**
     * GZIP 压缩字符串, 并且BASE64编码
     * 
     * @param inputStream
     * @param charsetName
     * @return
     * @throws IOException
     */
    public static String compressInputStream2String(InputStream inputStream, String charsetName) throws IOException {
        return Base64.encodeBase64String(compressInputStream(inputStream, charsetName));
    }

    /**
     * BASE64解码,并且GZIP解压缩字符串（utf-8）
     * 
     * @param String str
     * @return String
     * @throws IOException
     */
    public static String uncompressString(String str) throws IOException {
        return uncompressString(str, "UTF-8");
    }

    /**
     * BASE64解码,并且GZIP解压缩字符串
     * 
     * @param String str
     * @param String charsetName
     * @return String
     * @throws IOException
     */
    public static String uncompressString(String str, String charsetName) throws IOException {
        if (str == null || str.trim().length() == 0) {
            return str;
        }
        return uncompress(Base64.decodeBase64(str), charsetName);
    }

    /**
     * GZIP解压缩（utf-8）
     * 
     * @param byte[] bytes
     * @return String
     * @throws IOException
     */
    public static String uncompress(byte[] bytes) throws IOException {
        return uncompress(bytes, "UTF-8");
    }

    /**
     * GZIP解压缩
     * 
     * @param byte[] bytes
     * @param String charsetName
     * @return String
     * @throws IOException
     */
    public static String uncompress(byte[] bytes, String charsetName) throws IOException {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPInputStream gunzip = new GZIPInputStream(new ByteArrayInputStream(bytes));
        byte[] buffer = new byte[1024];
        int offset = -1;
        while ((offset = gunzip.read(buffer)) != -1) {
            out.write(buffer, 0, offset);
        }
        gunzip.close();
        return out.toString(charsetName);
    }

    /**
     * GZIP解压缩,utf-8
     * 
     * @param InputStream inputStream
     * @return String
     * @throws IOException
     */
    public static String uncompress(InputStream inputStream) throws IOException {
        return uncompress(inputStream, "UTF-8");
    }

    /**
     * GZIP解压缩
     * 
     * @param InputStream inputStream
     * @param String charsetName
     * @return String
     * @throws IOException
     */
    public static String uncompress(InputStream inputStream, String charsetName) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPInputStream gunzip = new GZIPInputStream(inputStream);
        byte[] buffer = new byte[1024];
        int offset = -1;
        while ((offset = gunzip.read(buffer)) != -1) {
            out.write(buffer, 0, offset);
        }
        gunzip.close();
        return out.toString(charsetName);
    }

    /**
     * 先BASE64解码，再GZIP解压缩
     * 
     * @param str
     * @return
     * @throws IOException
     */
    public static byte[] uncompress(String str) throws IOException {
        byte[] bytes = Base64.decodeBase64(str);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPInputStream gunzip = new GZIPInputStream(new ByteArrayInputStream(bytes));
        byte[] buffer = new byte[1024];
        int offset = -1;
        while ((offset = gunzip.read(buffer)) != -1) {
            out.write(buffer, 0, offset);
        }
        gunzip.close();
        return out.toByteArray();
    }

    /**
     * 使用zip进行压缩
     * 
     * @param str 压缩前的文本
     * @return 返回压缩后的文本
     * @throws IOException
     */
    public static String zip(String str) throws IOException {
        if (str == null || str.trim().length() == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ZipOutputStream zout = new ZipOutputStream(out);

        zout.putNextEntry(new ZipEntry("0"));
        zout.write(str.getBytes());
        zout.closeEntry();
        zout.close();
        return Base64.encodeBase64String(out.toByteArray());
    }

    /**
     * 使用zip进行解压缩
     * 
     * @param compressed 压缩后的文本
     * @return 解压后的字符串
     * @throws IOException
     */
    public static String unzip(String str) throws IOException {
        if (str == null || str.trim().length() == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ZipInputStream zin = new ZipInputStream(new ByteArrayInputStream(Base64.decodeBase64(str)));
        zin.getNextEntry();

        byte[] buffer = new byte[1024];
        int offset = -1;
        while ((offset = zin.read(buffer)) != -1) {
            out.write(buffer, 0, offset);
        }
        zin.close();
        return out.toString();
    }

}
