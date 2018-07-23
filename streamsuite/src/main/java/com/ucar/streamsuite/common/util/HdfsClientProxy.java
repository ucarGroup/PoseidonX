package com.ucar.streamsuite.common.util;

import com.ucar.streamsuite.common.constant.StreamContant;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URI;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.List;

/**
 * Description: 访问hdfs的工具类
 * Created on 2018/1/18 下午4:33
 *
 */
public class HdfsClientProxy {
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsClientProxy.class);

    public static  boolean confInit=false;
    private static Configuration conf = new HdfsConfiguration();
    static{
        initHadoopConf();
    }

    private static Configuration initHadoopConf() {
        if(!confInit){

            System.setProperty("HADOOP_USER_NAME", StreamContant.HADOOP_USER_NAME);
            conf.set("fs.defaultFS", YarnClientProxy.getConf().get("fs.default.name"));
            conf.set("ha.zookeeper.quorum", YarnClientProxy.getConf().get("yarn.resourcemanager.zk-address"));
            conf.set("dfs.replication", YarnClientProxy.getConf().get("dfs.replication"));
            confInit=true;
        }
        return conf;
    }

    /**
     * 获得FileSystem
     * @return
     */
    public static FileSystem getHadoopFS(){
        UserGroupInformation userGroupInformation = UserGroupInformation.createRemoteUser(StreamContant.HADOOP_USER_NAME);
        return userGroupInformation.doAs(new PrivilegedAction<FileSystem>(){
            @Override
            public FileSystem run() {
                try {
                    return FileSystem.get(conf);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return null;
            }
        });
    }

    /**
     * 向指定HDFS写文本内容
     * @param file
     * @param message
     */
    public static void writeMessageToHDFSFile(String file, String message) {
        FSDataOutputStream out=null;
        FileSystem fs=null;
        try{
            fs = FileSystem.get(URI.create(file), conf);
            Path path = new Path(file);
            out = fs.create(path);   //创建文件
            out.writeBytes(message);
            out.write(message.getBytes("UTF-8"));
            out.close();
        }catch (Exception e){
            LOGGER.error("writeMessageToHDFSFile is error", e);
        }finally {
            try {
                if(out!=null){
                    out.close();
                }
                if(fs != null){
                    fs.close();
                }
            } catch (IOException e) {
            }
        }
    }

    /**
     * 下载文件
     * @param localFilePath
     * @param hdfsPath
     */
    public static void downloadFileToLocal(String localFilePath,String hdfsPath){
        FileSystem hadoopFS = null;
        try {
            initHadoopConf();
            hadoopFS = getHadoopFS();
            InputStream in = hadoopFS.open(new Path(hdfsPath));
            OutputStream out = new FileOutputStream(localFilePath);
            IOUtils.copyBytes(in, out, 4096, true);
        }catch (Exception e) {
            LOGGER.error("downloadFileFromHdfs is error", e);
        }finally {

            if(hadoopFS != null){
                try {
                    hadoopFS.close();
                }catch (Exception e) {
                }
            }
        }
    }

    /**
     * hdfs上创建目录
     * @param hdfsPath
     * @return
     */
    public static void createPath(String hdfsPath){
        FileSystem hadoopFS = null;
        try {
            initHadoopConf();
            hadoopFS = getHadoopFS();
            hadoopFS.mkdirs(new Path(hdfsPath));
        }catch (Exception e) {
            LOGGER.error("createPathDir is error", e);
        }finally {
            if(hadoopFS != null){
                try {
                    hadoopFS.close();
                }catch (Exception e) {
                }
            }
        }
    }

    /**
     * 获得hdfs文件的列表
     * @param hdfsPath
     * @return
     */
    public static List<String> getHdfsFileList(String hdfsPath){
        FileSystem hadoopFS = null;
        FSDataOutputStream fsOut = null;
        FSDataInputStream fsIn = null;
        List<String> fileNameList = new ArrayList<String>();
        try {
            initHadoopConf();
            hadoopFS = getHadoopFS();
            FileStatus[] status = hadoopFS.listStatus(new Path(hdfsPath));
            for (FileStatus file : status) {
                fileNameList.add(file.getPath().getName());
            }
        }catch (Exception e) {
            LOGGER.error("getHdfsFileList is error", e);
        }finally {
            if(fsIn != null){
                try {
                    fsIn.close();
                }catch (Exception e) {
                }
            }
            if(fsOut != null){
                try {
                    fsOut.close();
                }catch (Exception e) {
                }
            }
            if(hadoopFS != null){
                try {
                    hadoopFS.close();
                }catch (Exception e) {
                }
            }
        }
        return fileNameList;
    }

    /**
     * 删除文件
     * @param hdfsPath
     * @return
     */
    public static boolean deleteFile(String hdfsPath){
        FileSystem hadoopFS = null;
        try {
            initHadoopConf();
            hadoopFS = getHadoopFS();
            hadoopFS.delete(new Path(hdfsPath), false);
            return true;
        }catch (Exception e) {
            LOGGER.error("deleteFile is error", e);
            return true;
        }finally {
            if(hadoopFS != null){
                try {
                    hadoopFS.close();
                }catch (Exception e) {
                }
            }
        }
    }

    /**
     * 从本地传文件到hdfs
     * @param localFilePath
     * @param hdfsPath
     */
    public static boolean uploadFile2Hdfs(String localFilePath,String hdfsPath){
        final int BUFFER_SIZE = 8*1024*1024;
        InputStream in = null;
        FileSystem hadoopFS = null;
        FSDataOutputStream fsOut = null;
        try {
            initHadoopConf();
            in = new FileInputStream(localFilePath);

            hadoopFS = getHadoopFS();
            fsOut = hadoopFS.create(new Path(hdfsPath));

            byte[] bytes = new byte[BUFFER_SIZE];
            int  readBytes = in.read(bytes);
            while (readBytes > 0) {
                fsOut.write(bytes, 0, readBytes);
                readBytes = in.read(bytes);
            }
        }catch (Exception e) {
            LOGGER.error("uploadFile2Hdfs is error", e);
            return false;
        }finally {
            if(in != null){
                try {
                    in.close();
                }catch (Exception e) {
                }
            }
            if(fsOut != null){
                try {
                    fsOut.close();
                }catch (Exception e) {
                }
            }
            if(hadoopFS != null){
                try {
                    hadoopFS.close();
                }catch (Exception e) {
                }
            }
        }
        return true;
    }

    public static boolean checkFileExists(String path) {
        FileSystem fs =null;
        try {
            fs = FileSystem.get(conf);
            Path srcPath = new Path(path);
            return fs.exists(srcPath);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(),e);
        }finally {
            try {
                fs.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
            fs=null;
        }
        return false;
    }

    public static Configuration getConf(){
        return conf;
    }
}
