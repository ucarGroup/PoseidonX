package com.ucar.streamsuite.common.util;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.SCPClient;
import ch.ethz.ssh2.SFTPv3Client;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;

import com.ucar.streamsuite.common.exception.SSHException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


/**
 * Description:
 * Created on 2018/1/30 上午10:06
 *
 */
public class SSHExecutor {
    public static final Logger LOGGER = LoggerFactory.getLogger(SSHExecutor.class);

    /**
     * 连接timeout，单位：毫秒
     */
    private static final int CONNCET_TIMEOUT = 5000;

    /**
     * 操作timeout
     */
    private static final int OPERATE_TIMEOUT = 30000;

    /**
     * 回车换行
     */
    private static final String CRLF = "\r\n";

    /**
     * 任务线程池
     */
    private static ThreadPoolExecutor taskPool = new ThreadPoolExecutor(
            32, 128, 0L, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<Runnable>(1000),
            new NamedThreadFactory("ssh", true));


    /**
     * 通过回调执行命令
     *
     * @param ip       目标服务器ip
     * @param port     目标服务器port
     * @param username 用户名
     * @param password 密码
     * @param callback 回调接口
     * @throws SSHException
     */
    public Result execute(String ip, int port, String username, String password,
                          SSHCallback callback) throws SSHException {
        Connection conn = null;
        try {
            conn = getConnection(ip, port, username, password);
            return callback.call(new SSHSession(conn, ip + ":" + port));
        } catch (Exception e) {
            throw new SSHException("SSH error: " + e.getMessage(), e);
        } finally {
            close(conn);
        }
    }

    /**
     * 获取连接并校验
     *
     * @param ip       目标服务器ip
     * @param port     目标服务器port
     * @param username 用户名
     * @param password 密码
     * @throws Exception
     */
    private Connection getConnection(String ip, int port,
                                     String username, String password) throws Exception {
        Connection conn = new Connection(ip, port);
        conn.connect(null, CONNCET_TIMEOUT, CONNCET_TIMEOUT);
        boolean isAuthenticated = conn.authenticateWithPassword(username, password);
        if (!isAuthenticated) {
            throw new Exception("SSH authentication failed with [ userName: " +
                    username + ", password: " + password + "]");
        }
        return conn;
    }


    /**
     * 执行命令回调
     */
    public interface SSHCallback {
        /**
         * 执行回调
         *
         * @param session
         */
        Result call(SSHSession session);
    }

    /**
     * 可以调用多次executeCommand， 并返回结果
     */
    public class SSHSession {

        private String address;
        private Connection conn;

        private SSHSession(Connection conn, String address) {
            this.conn = conn;
            this.address = address;
        }

        /**
         * 执行命令并返回结果，可以执行多次
         *
         * @param cmd 命令
         * @return 执行成功Result为true，并携带返回信息,返回信息可能为null
         * 执行失败Result为false，并携带失败信息
         * 执行异常Result为false，并携带异常
         */
        public Result executeCommand(String cmd) {
            return executeCommand(cmd, OPERATE_TIMEOUT);
        }

        public Result executeCommand(String cmd, int timoutMillis) {
            return executeCommand(cmd, null, timoutMillis);
        }

        public Result executeCommand(String cmd, LineProcessor lineProcessor) {
            return executeCommand(cmd, lineProcessor, OPERATE_TIMEOUT);
        }

        /**
         * 执行命令并返回结果，可以执行多次
         *
         * @param cmd           命令
         * @param lineProcessor 回调处理行
         * @return 如果lineProcessor不为null, 那么永远返回Result.true
         */
        public Result executeCommand(String cmd, LineProcessor lineProcessor, int timeoutMillis) {
            Session session = null;
            try {
                session = conn.openSession();
                return executeCommand(session, cmd, timeoutMillis, lineProcessor);
            } catch (Exception e) {
                LOGGER.error("execute ip:" + conn.getHostname() + " cmd:" + cmd, e);
                return new Result(e);
            } finally {
                close(session);
            }
        }

        /**
         * 执行命令并返回结果
         *
         * @param session
         * @param cmd
         * @param timeoutMillis
         * @param lineProcessor
         * @return
         * @throws Exception
         */
        public Result executeCommand(final Session session, final String cmd,
                                     final int timeoutMillis, final LineProcessor lineProcessor) throws Exception {
            Future<Result> future = taskPool.submit(new Callable<Result>() {
                public Result call() throws Exception {
                    session.execCommand(cmd);
                    //如果客户端需要进行行处理，则直接进行回调
                    if (lineProcessor != null) {
                        processStream(session.getStdout(), lineProcessor);
                    } else {
                        //获取标准输出
                        String rst = getResult(session.getStdout());
                        if (rst != null) {
                            return new Result(true, rst);
                        }
                        //返回为null代表可能有异常，需要检测标准错误输出，以便记录日志
                        Result errResult = tryLogError(session.getStderr(), cmd);
                        if (errResult != null) {
                            return errResult;
                        }
                    }
                    return new Result(true, null);
                }
            });
            Result rst = null;
            try {
                rst = future.get(timeoutMillis, TimeUnit.MILLISECONDS);
                future.cancel(true);
            } catch (TimeoutException e) {
                LOGGER.error("exec ip:{} {} timeout:{}", conn.getHostname(), cmd, timeoutMillis);
                throw new SSHException(e);
            }
            return rst;
        }

        private Result tryLogError(InputStream is, String cmd) {
            String errInfo = getResult(is);
            if (errInfo != null) {
                LOGGER.error("address " + address + " execute cmd:({}), err:{}", cmd, errInfo);
                return new Result(false, errInfo);
            }
            return null;
        }

        /**
         * 获取远程文件内容
         *
         * @param remoteFile 远程文件名称，包含路径
         * @return
         */
        public Result getRemoteFile(String remoteFile) {
            try {
                SCPClient client = conn.createSCPClient();
                OutputStream outputStream = new ByteArrayOutputStream();
                client.get(remoteFile, outputStream);
                return new Result(true, outputStream.toString());
            } catch (Exception e) {
                LOGGER.error("Get remoteFile {} error.", remoteFile, e);
                return new Result(e);
            }
        }

        /**
         * 写远程文件
         *
         * @param remoteDir 远程文件路径
         * @param fileName  远程文件名称
         * @param content   文件内容
         * @return
         */
        public Result writeRemoteFile(String remoteDir, String fileName, String content) {
            try {
                if (StringUtils.isNotBlank(content)) {
                    SCPClient client = conn.createSCPClient();
                    client.put(content.getBytes(), fileName, remoteDir);
                    return new Result(true);
                }
            } catch (Exception e) {
                LOGGER.error("Write {} to remoteFile {}/{} error.", content, remoteDir, fileName);
                return new Result(e);
            }
            return new Result(false);
        }

        /**
         * 复制本地的文件至远程服务器
         *
         * @param remoteDir 远程服务器路径
         * @param localPath 本地文件路径
         * @return
         */
        public Result scpFileToRemote(String remoteDir, String localPath) {
            try {
                SCPClient scpClient = conn.createSCPClient();
                scpClient.put(localPath, remoteDir, "0644");
                return new Result(true);
            } catch (Exception e) {
                LOGGER.error("scp file {} to remote dir {} error.", localPath, remoteDir);
                return new Result(e);
            }
        }

        /**
         * 创建目录
         *
         * @param remoteDir 待创建目录
         * @return
         */
        public Result mkDir(String remoteDir) {
            try {
                SFTPv3Client sftpClient = new SFTPv3Client(conn);
                // 权限全部给创建者
                sftpClient.mkdir(remoteDir, 0700);
                return new Result(true);
            } catch (Exception e) {
                LOGGER.error("Make remote dir {} failed.", remoteDir, e);
                return new Result(e);
            }
        }
    }

    /**
     * 获取调用命令后的返回结果
     *
     * @param is 输入流
     * @return 如果获取结果有异常或者无结果，那么返回null
     */
    private String getResult(InputStream is) {
        final StringBuffer buffer = new StringBuffer();
        LineProcessor lp = new DefaultLineProcessor() {
            public void process(String line, int lineNum) throws Exception {
                if (lineNum > 1) {
                    buffer.append(CRLF);
                }
                buffer.append(line);
            }
        };
        processStream(is, lp);
        return buffer.length() > 0 ? buffer.toString() : null;
    }

    /**
     * 从流中获取内容
     *
     * @param is 输入流
     */
    private void processStream(InputStream is, LineProcessor lineProcessor) {
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(new StreamGobbler(is)));
            String line = null;
            int lineNum = 1;
            while ((line = reader.readLine()) != null) {
                try {
                    lineProcessor.process(line, lineNum);
                } catch (Exception e) {
                    LOGGER.error("err line:" + line, e);
                }
                lineNum++;
            }
            lineProcessor.finish();
        } catch (IOException e) {
            LOGGER.error(e.getMessage(), e);
        } finally {
            close(reader);
        }
    }

    public static abstract class DefaultLineProcessor implements LineProcessor {
        public void finish() {
        }
    }

    /**
     * 从流中直接解析数据
     */
    public interface LineProcessor {
        /**
         * 处理行
         *
         * @param line    内容
         * @param lineNum 行号，从1开始
         * @throws Exception
         */
        void process(String line, int lineNum) throws Exception;

        /**
         * 所有的行处理完毕回调该方法
         */
        void finish();
    }

    /**
     * ssh结果类
     */
    public class Result {

        private boolean success;
        private String result;
        private Exception excetion;

        public Result(boolean success) {
            this.success = success;
        }

        public Result(boolean success, String result) {
            this.success = success;
            this.result = result;
        }

        public Result(Exception excetion) {
            this.success = false;
            this.excetion = excetion;
        }

        public Exception getExcetion() {
            return excetion;
        }

        public void setExcetion(Exception excetion) {
            this.excetion = excetion;
        }

        public boolean isSuccess() {
            return success;
        }

        public void setSuccess(boolean success) {
            this.success = success;
        }

        public String getResult() {
            return result;
        }

        public void setResult(String result) {
            this.result = result;
        }

        @Override
        public String toString() {
            return "Result{" +
                    "success=" + success +
                    ", result='" + result + '\'' +
                    ", excetion=" + excetion +
                    '}';
        }
    }

    /**
     * 关闭BufferedReader
     *
     * @param read BufferedReader
     */
    private void close(BufferedReader read) {
        if (read != null) {
            try {
                read.close();
            } catch (IOException e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 关闭connection
     *
     * @param conn 连接
     */
    private void close(Connection conn) {
        if (conn != null) {
            try {
                conn.close();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    /**
     * 关闭session
     *
     * @param session session
     */
    private static void close(Session session) {
        if (session != null) {
            try {
                session.close();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }
}
