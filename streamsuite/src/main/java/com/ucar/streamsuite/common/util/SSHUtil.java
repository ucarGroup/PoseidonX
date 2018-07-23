package com.ucar.streamsuite.common.util;

import com.ucar.streamsuite.common.exception.SSHException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Description: 登录远程服务器，并发送shell命令
 * Created on 2018/1/30 上午10:05
 *
 */
public class SSHUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(SSHUtil.class);
    private final static SSHExecutor SSH_EXECUTOR = new SSHExecutor();


    /**
     * SSH 方式登录远程主机，执行命令
     *
     * @param host          远程主机ip
     * @param port          远程主机port
     * @param username      用户名
     * @param password      密码
     * @param command       命令
     * @param successReturn ssh成功才返回结果
     * @return
     * @throws SSHException
     */
    public static String execute(String host, int port, String username, String password,
                                 final String command, boolean successReturn) throws SSHException {
        if (StringUtils.isBlank(command)) {
            return StringUtils.EMPTY;
        }
        SSHExecutor.Result result = SSH_EXECUTOR.execute(host, port, username, password,
                new SSHExecutor.SSHCallback() {
                    public SSHExecutor.Result call(SSHExecutor.SSHSession session) {
                        return session.executeCommand(command);
                    }
                });
        // 要求执行成功才返回结果时，失败返回空
        if (result == null || successReturn && !result.isSuccess()) {
            return StringUtils.EMPTY;
        }
        return result.getResult();
    }



}
