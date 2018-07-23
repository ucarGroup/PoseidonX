package com.ucar.streamsuite.common.dto;

import java.io.Serializable;

/**
 * Description:通用的操作结果dto
 * Created on 2018/1/31 下午2:36
 *
 */
public class OperResultDTO implements Serializable {

    private static final long serialVersionUID = -5729044331250315760L;

    private boolean result; //操作成功(true)还是失败(false)
    private String errMsg; //返回的失败信息
    private String msg; //返回的普通消息

    public boolean isResult() {
        return result;
    }

    public void setResult(boolean result) {
        this.result = result;
    }

    public String getErrMsg() {
        return errMsg;
    }

    public void setErrMsg(String errMsg) {
        this.errMsg = errMsg;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
