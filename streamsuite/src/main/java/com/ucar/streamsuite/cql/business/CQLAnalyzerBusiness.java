package com.ucar.streamsuite.cql.business;

import com.huawei.streaming.cql.CQLClient;
import com.huawei.streaming.cql.CQLSessionState;
import com.ucar.streamsuite.engine.constants.EngineContant;
import java.util.List;

/**
 * Created on 2017/1/23 上午10:18:18
 *
 */
public class CQLAnalyzerBusiness {

    public static String analyze(String cqlText){

        String checkResult = "";

        //屏蔽无用的日志日志
        CQLClient.disableLog4j();
        CQLClient client = new CQLClient();
        client.getCustomizedConfigurationMap().put(EngineContant.RTCP_CQL_IS_SUBMIT,false);
        if (client.initSessionState() != CQLSessionState.STATE_OK)
        {
            checkResult = "init cql session error";
            return checkResult;
        }
        List<String> sqls = CQLUtils.analyzeContent(cqlText);
        String errorCQL = "";

        for (int i = 0; i < sqls.size(); i++)
        {
            String tmp = client.checkCQL(sqls.get(i));
            if(tmp.length() >0  ) {
                tmp += "<br/>";
                checkResult += tmp;
                if(!sqls.get(i).trim().toLowerCase().startsWith("submit")) {
                    errorCQL = "错误CQL语句:\n" + sqls.get(i);
                }
            }
        }

        if(checkResult.length() == 0){
            checkResult = "CQL 语法校验 没有问题!";
        } else {
            checkResult = checkResult.replaceAll("\n", "<br/>");
            errorCQL = errorCQL.replaceAll("\n", "<br/>");
            checkResult = errorCQL + "<br/>" + checkResult ;
        }
        return checkResult;
    }
}

