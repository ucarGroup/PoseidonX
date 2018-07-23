/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.exception;

import java.text.MessageFormat;

import com.huawei.streaming.udfs.UDF;

/**
 * Streaming编译期所产生的异常的各种异常码
 * <p/>
 * CQL异常码的设计
 * 异常码分为三部分:
 * 前缀：以CQL-开头
 * 分类：两位数字编号，从00开头
 * 细分：三位数字编号，从000开头
 * <p/>
 * 错误码 SQLSTATE 异常描述
 * CQL-00 成功完成
 * CQL-01 警告
 * CQL-02 配置属性异常
 * CQL-03 流处理平台异常
 * CQL-04 语义分析异常,包含语法解析异常
 * CQL-05 拓扑异常，包含Streaming算子解析异常
 * CQL-06 函数解析异常
 * CQL-07 窗口解析异常
 * CQL-08 安全异常
 * CQL-99 未知异常
 *
 *  不支持指的是后续可能支持
 *  不允许是以后再也不可能修改
 *
 */
public enum ErrorCode
{
    // CQL-00 成功完成
    /**
     * 成功
     */
    SUCCESS("00000", "Success."),
    
    // CQL-01 警告
    /**
     * 系统的警告，为后期预留，目前并没有用到
     */
    WARNING("01000", "Warning {0}."),
    
    // CQL-02 配置属性异常
    /**
     * 找不到配置属性
     */
    CONFIG_NOT_FOUND("02000", "Cannot find config ''{0}''."),
    
    /**
     * 配置属性数据类型转换失败
     * 配置属性在CQL中设置的时候，都是String类型，
     * 但是在具体使用的时候，需要的类型可能不一致，这就需要进行类型转换。
     * 该异常码就发生在配置属性在使用的时候，进行类型转换失败的场景。
     */
    CONFIG_FORMAT("02001", "Failed to convert config value ''{0}'' to type ''{1}''."),
    
    
    /**
     * 配置属性错误的值，通常发生在数据类型正确，但是里面的值不符合具体要求。
     */
    CONFIG_VALUE_ERROR("02002", "Failed to read config value ''{0}''."),
    
    // CQL-03 流处理平台异常
    /**
     * 删除应用程序超时
     */
    PLATFORM_KILL_OVERTIME("03000", "Kill application timeout."),
    
    /**
     * 应用程序已经存在
     */
    PLATFORM_APP_EXISTS("03001", "Application ''{0}'' already exist."),
    
    /**
     * 应用程序不存在
     */
    PLATFORM_APP_NOT_EXISTS("03002", "Application ''{0}'' does not exist."),
    
    /**
     * 应用程序的算子拓扑异常
     */
    PLATFORM_INVALID_TOPOLOGY("03003", "The submit topology is invalid."),
    
    /**
     * 找不到输入算子
     */
    PLATFORM_NO_INPUT_OPERATOR("03004", "Cannot find input operators."),
    
    /**
     * 应用程序状态异常
     */
    PLATFORM_APP_STATUS_ERROR("03005","The status of application ''{0}'' is ''{1}'', ''{2}'' failed."),
    
    /**
     * 应用程序worker数量设置异常
     */
    PLATFORM_INVALID_WORKER_NUMBER("03006",
        "The worker number is invalid, the available slot number is ''{0}'', the executor number is ''{1}''."),
    
    /**
     * 无效的nimbus服务异常
     */
     PLATFORM_INVALID_NIMBUS_ERROR("03007","Current nimbus is not a leader."),    
        
     /**
      * 服务端通信异常
      */
      PLATFORM_NIMBUS_SERVER_EXCEPTION("03008","Communication error by using thrift."),   
      
    // CQL-04 语义分析异常,包含语法解析异常
    /**
     * 语法解析异常
     */
    SEMANTICANALYZE_PARSE_ERROR("04000", "{0}."),
    
    /**
     * 不支持的语法
     */
    SEMANTICANALYZE_UNSUPPORTED_GRAMMAR("04001", "Grammar ''{0}'' is not supported now."),
    
    /**
     * order by 子句中只支持属性表达式
     */
    SEMANTICANALYZE_ORDERBY_EXPRESSION_UNSPPORTED("04002",
        "Only property value expression is supported in order by expression."),
    
    /**
     * 表达式在select语句中找不到
     */
    SEMANTICANALYZE_NO_EXPRESSION_IN_SELECT("04003", "Cannot find expression ''{0}'' in select clause."),
    
    //CQL原始命令解析错误
    /**
     * 地址文件不存在
     */
    SEMANTICANALYZE_FILE_NOT_EXISTS("04004", "File in path ''{0}'' does not exist."),
    
    /**
     * 地址必须是一个文件
     */
    SEMANTICANALYZE_FILE_IS_DIR("04005", "File in path ''{0}'' must be a file type."),
    
    /**
     * 文件不是jar包类型
     */
    SEMANTICANALYZE_FILE_NOT_JAR("04006", "File type ''{0}'' is not a jar type."),
    
    /**
     * Jar包地址无法解析
     */
    SEMANTICANALYZE_REGISTER_JAR("04007", "Failed to register jar, path ''{0}'' cannot be parsed."),
    
    //Schema、数据类型、常量相关
    /**
     * 找不到对应流
     */
    SEMANTICANALYZE_NOFOUND_STREAM("04008", "Cannot find stream ''{0}''."),
    
    /**
     * 常量类型转换失败
     */
    SEMANTICANALYZE_CONSTANT_FORMAT("04009", "Cannot format constant value ''{0}'' to ''{1}'' type."),
    
    /**
     * 不支持的数据类型
     */
    SEMANTICANALYZE_UNSUPPORTED_DATATYPE("04010", "Unsupported data type ''{0}''."),
    
    /**
     * 无法从指定流中找到对应列
     */
    SEMANTICANALYZE_NO_COLUMN("04011", "Cannot find column ''{0}'' in stream ''{1}''."),
    
    /**
     * 无法从所有的流中找到对应列
     */
    SEMANTICANALYZE_NO_COLUMN_ALLSTREAM("04012", "Cannot find column ''{0}'' in related streams."),
    
    /**
     * 无法唯一确定列, 该列可能存在在多个流中
     */
    SEMANTICANALYZE_DUPLICATE_COLUMN_ALLSTREAM("04013", "There is more than one column ''{0}'' in related streams."),
    
    /**
     * 无法根据名称找到对应流
     */
    SEMANTICANALYZE_NO_STREAM("04014", "Cannot find stream name ''{0}''."),
    
    /**
     * 输出列数量和select子句的列数量不一致
     */
    SEMANTICANALYZE_NOTSAME_COLUMNS("04015",
        "Select output column size is ''{0}'', but find ''{1}'' columns in output stream."),
    
    //表达式
    /**
     * like表达式的子表达式必须是String类型
     */
    SEMANTICANALYZE_LIKE_STRING("04016", "All child expressions in ''LIKE'' expression must be ''STRING'' type."),
    
    /**
     * not表达式中所有的表达式返回值必须是boolean类型
     */
    SEMANTICANALYZE_NOT_EXPRESSION_BOOLEAN_TYPE("04017",
        "All expressions in ''NOT'' expression must be ''BOOLEAN'' type."),
    
    /**
     * IN和BETWEEN表达式中的参数只支持常量
     */
    SEMANTICANALYZE_IN_BETWEEN_EXPRESSION("04018",
        "Only constant expression is supported in ''IN'' and ''BETWEEN'' expressions."),
    
    /**
     * CASE-WHEN表达式必须至少包含一个WHEN-THEN表达式
     */
    SEMANTICANALYZE_CASE_WHEN_MORE_WHEN_THEN("04019",
        "''CASE-WHEN'' expression must have one or more ''WHEN-THEN'' expression."),
    
    /**
     * WHEN-THEN表达式必须同时包含WHEN和THEN表达式
     */
    SEMANTICANALYZE_CASE_WHEN_MUST_WHEN_THEN("04020",
        "''WHEN-THEN'' expression must have ''WHEN'' expression and ''THEN'' expression."),
    
    /**
     * WHEN-THEN表达式的WHEN表达式返回值必须是boolean类型
     */
    SEMANTICANALYZE_CASE_WHEN_WHEN_BOOLEAN("04021",
        "''WHEN'' expression in ''WHEN-THEN'' expression must be ''BOOLEAN'' type."),
    
    /**
     * CASE表达式必须包含输入的表达式
     */
    SEMANTICANALYZE_CASE_WHEN_WHEN_MUST("04022", "''CASE'' expression must have input expression."),
    
    /**
     * 算术表达式只支持number的数据类型
     */
    SEMANTICANALYZE_ARITHMETIC_EXPRESSION_NUMBER_TYPE("04023",
        "All expressions in arithmetic expression must be number type."),
    
    /**
     * 逻辑表达式中的表达式必须是boolean类型
     */
    SEMANTICANALYZE_LOGIC_EXPRESSION_BOOLEAN_TYPE("04024",
        "All expressions in logic expression must be ''BOOLEAN'' type."),
    
    //数据源算子
    /**
     * 无法找到对应的数据源
     */
    SEMANTICANALYZE_DATASOURCE_UNKNOWN("04025", "Unknown dataSource ''{0}''."),
    
    /**
     * 从数据源参数中找不到对应表达式
     */
    SEMANTICANALYZE_DATASOURDE_NO_ARGUMENT("04026", "Cannot find expression ''{0}'' in dataSource arguments."),
    
    //combine算子
    /**
     * combine算子不允许出现窗口
     */
    SEMANTICANALYZE_COMBINE_WINDOW("04027", "Window is not allowed in combine clause."),
    
    /**
     * combine算子的输入流和combine条件中的流数量不匹配
     */
    SEMANTICANALYZE_COMBINE_SIZE("04028",
        "Combine stream size is ''{0}'', but combine condition stream size is ''{1}''."),
    
    /**
     * combine算子只支持属性表达式
     */
    SEMANTICANALYZE_COMBINE_SIMPLE_EXPRESSION("04029", "Only property value expression is allowed in combine clause."),
    
    /**
     * combine语句中，一个流的输出列在select语句中必须放在一起
     */
    SEMANTICANALYZE_COMBINE_EXPRESSION_TOGETHER("04030",
        "Columns in same stream must be together in combine select clause."),
    
    //多输出算子
    /**
     * MultiInsert中不允许出现数据源
     */
    SEMANTICANALYZE_MULTIINSERT_DATASOURCE("04031", "DataSource is not allowed in multi insert clause."),
    
    /**
     * MultiInsert中不允许出现GroupBy
     */
    SEMANTICANALYZE_MULTIINSERT_GROUPBY("04032", "Group by is not allowed in multi insert statement."),
    
    /**
     * MultiInsert中不允许出现窗口
     */
    SEMANTICANALYZE_MULTIINSERT_WINDOW("04033", "Window is not allowed in multi insert statement."),
    
    /**
     * MultiInsert中不允许出现多个输入流
     */
    SEMANTICANALYZE_MULTIINSERT_JOIN("04034", "Only one input stream is allowed in multi insert statement."),
    
    //Join算子
    /**
     * 不支持多流Join
     */
    SEMANTICANALYZE_MULTI_JOIN("04035", "Only two streams are allowed in join."),
    
    /**
     * NATURAL JOIN 不支持
     */
    SEMANTICANALYZE_JOIN_UNSPPORTTED_NATURAL_JOIN("04036", "''NATURAL JOIN'' is not supported."),
    
    /**
     * Join中，只允许其中一个流设置unidirection属性
     */
    SEMANTICANALYZE_UNIDIRECTION_ONLY_ONE("04037", "Only one stream can be set unidirection property."),
    
    /**
     * join语句中找不到On条件
     */
    SEMANTICANALYZE_JOIN_NO_CONDITION("04038", "Cannot find join condition in join clause."),
    
    /**
     * join的条件中的表达式在在输入流中找不到对应列
     */
    SEMANTICANALYZE_JOIN_NO_COLUMN("04039", "Cannot find column in stream from join condition."),
    
    /**
     * 不支持的Join条件
     */
    SEMANTICANALYZE_UNSPPORTED_JOIN_CONDITION("04040", "Unsupported join condition {0}."),
    
    /**
     * 找不到序列化和反序列化类
     */
    SEMANTICANALYZE_UNKNOWN_SERDE("04041", "Cannot find serialize/deserialize class."),
    
    /**
     * 找不到类
     */
    SEMANTICANALYZE_UNKOWN_CLASS("04042", "Cannot find class ''{0}''."),
    
    /**
     * 序列化或者反序列化类 初始化失败
     */
    SEMANTICANALYZE_SERDE_INITERROR("04043", "Failed to initialize serializer/deserializer ''{0}''."),
    
    /**
     * 流名称已经存在
     */
    SEMANTICANALYZE_EXISTS_STREAM("04044", "Stream name ''{0}'' already exists."),
    
    /**
     * 表达式比较异常，一般是两个表达式类型不匹配，无法比较
     * 比如String和Int类型的比较
     */
    SEMANTICANALYZE_EXPRESSION_DATATYPE_COMPARE("04045", "DataType ''{0}'' and ''{1}'' cannot compare."),

    /**
     * 用户文件大小超过最大限制
     */
    SEMANTICANALYZE_USERFILE_OVER_MAXSIZE("04046", "All size of user files over max size {0}MB limit."),

    /**
     * 解析上下文为空
     */
    SEMANTICANALYZE_CONTEXT_NULL("04047", "ParseContext is null."),

    /**
     * 窗口不支持exclude now功能
     */
    SEMANTICANALYZE_UNSUPPORTED_EXCLUDE_NOW("04048", "Exclude now is not supported in this window."),

    /**
     * previous不允许和排序窗口一起用
     */
    SEMANTICANALYZE_PREVIOUS_WITH_SORTWINDOW("04049", "''PREVIOUS'' can not be used with Sort window."),

    /**
     * join中不支持输出R流的窗口
     */
    SEMANTICANALYZE_UNSPPORTED_WINDOW_JOIN("04050", "RStream Window is not allowed in this join."),
    
    /**
     * selfJoin中只允许单流
     */
    SEMANTICANALYZE_UNIDIRECTION_SELFJOIN("04051", "Cannot find ''UNIDIRECTION'' in left and right stream."),
    
    /**
     * selfJoin中只允许单流
     */
    SEMANTICANALYZE_NO_USEROPERATOR("04052", "Cannot find user operator ''{0}''."),
    
    /**
     * 输入schema超过限制
     */
    SEMANTICANALYZE_OVER_INPUTSCHEMA("04053", "Only one input schema is allowed in user defined operator."),

    /**
     * 输出schema超过限制
     */
    SEMANTICANALYZE_OVER_OUTPUTSCHEMA("04054", "Only one output schema is allowed in user defined operator."),
    
    /**
     * 用户自定义算子中的输入schema校验失败
     */
    SEMANTICANALYZE_INVALID_INPUTSCHEMA("04055", "Invalid input schema in user defined operator."),
    
    /**
     * 用户自定义算子中的输出schema校验失败
     */
    SEMANTICANALYZE_INVALID_OUTPUTSCHEMA("04056", "Invalid output schema in user defined operator."),

    /**
     * 算子不匹配
     * 通常用于算子名称正确，但是却被使用在了错误的地方
     * 比如output位置使用了input算子
     */
    SEMANTICANALYZE_UNMATCH_OPERATOR("04057", "The ''{0}'' operator type does not match."),

    // CQL-05 拓扑异常，包含Streaming算子解析异常
    /**
     * 一个CQL文件只允许提交一个应用程序
     */
    TOP_ONE_FILE_ONE_TOP("05000", "Only one ''SUBMIT APPLICATION'' command is allowed in one cql file."),
    
    /**
     * 算子找不到输入流
     */
    TOP_TRANSITION_TO("05001", "Cannot find input stream to operator ''{0}''."),
    
    /**
     * 算子找不到输出流
     */
    TOP_TRANSITION_FROM("05002", "Cannot find output stream from operator ''{0}''."),
    
    /**
     * 应用程序名称为空
     */
    TOP_NO_NAME("05003", "Application name is null."),

    /**
     * 找不到执行计划文件
     */
    TOP_PHYSICPLAN_NOT_EXISTS("05004", "Cannot find physic plan in path ''{0}''."),

    /**
     * 执行计划文件内容错误
     */
    TOP_PHYSICPLAN_ERROR_CONTEXT("05005", "Unrecognized physic plan file context in path ''{0}''."),

    /**
     * 算子初始化失败
     */
    TOP_ERROR_INIT_OPERATOR("05006", "Failed to initialize ''{0}'' operator."),

    /**
     * 无法识别的时区
     */
    TOP_ERROR_TIME_ZONE("05007", "Unrecognized time zone ''{0}''."),

    // CQL-06 函数解析异常
    /**
     * 不支持的函数
     */
    FUNCTION_UNSPPORTED("06000", "Unknown function ''{0}''."),
    
    /**
     * 系统函数不能移除
     */
    FUNCTION_REMOVE_NATIVE("06001", "System function ''{0}'' cannot be dropped."),
    
    /**
     * 系统函数不能被覆盖
     */
    FUNCTION_OVERWRITE_NATIVE("06002", "System function ''{0}'' cannot be overwritten."),
    
    /**
     * 用户自定义函数必须继承自指定类
     */
    FUNCTION_ERROR_EXTENDS("06003", "User defined function ''{0}'' must extend from " + UDF.class.getName() + "."),
    
    /**
     * previous函数只能是PREVIOUS(number, column)这样的形式
     */
    FUNCTION_PREVIOUS_PROPERTYVALUE_EXPRSSION("06004", "Only ''PREVIOUS(number, column)'' is allowed."),

    /**
     * UDF函数中不支持的参数类型
     */
    FUNCTION_UNSUPPORTED_PARAMETERS("06005", "Unsupported parameters in function ''{0}''."),

    // CQL-07 窗口解析异常
    /**
     * 无法识别的窗口
     */
    WINDOW_UNRECGNIZE_WINDOW("07000", "Unknown window."),
    
    /**
     * exclude now中的select列表不能包含其他非group by列
     */
    WINDOW_EXCLUDE_GROUPONLY("07001",
        "All columns in select clause must be in group by clause when ''EXCLUDE NOW'' is used."),
    
    /**
      * 错误的窗口参数
      */
    WINDOW_UNSUPPORTED_PARAMETERS("07002", "Unsupported parameters when creating window."),     
    
    /**
     * 排序窗只允许滑动类型
     */
   WINDOW_SLIDEONLY_SORTWINDOW("07003", "Only slide type is allowed in sort window."),     
    
    // CQL-08 安全异常

    /**
     * 不支持的安全类型
     */
    SECURITY_UNSUPPORTED_TYPE("08000", "Unsupported security type ''{0}''."),
    
    /**
     * 没有授权
     */
    SECURITY_NO_GRANT_OPERATION("08001", "No grant."),
    
    /**
     * keytab文件地址错误
     */
    SECURITY_KEYTAB_PATH_ERROR("08002", "Invalid keytab path ''{0}''."),
    
    /**
     * 安全内部错误，例如：jaas文件地址错误
     */
    SECURITY_INNER_ERROR("08003", "Inner security error."),
    
    /**
     * 客户端用户鉴权失败
     */
    SECURITY_AUTHORIZATION_ERROR("08004", "Authorization failed."),
    
    // CQL-99 未知异常
    /**
     * 服务端通用异常
     */
    UNKNOWN_SERVER_COMMON_ERROR("99000", "An error occurred in CQL Engine."),
    
    /**
     * 未知异常
     */
    UNKNOWN_ERROR("99999", "Unknown error.");
    
    /**
     * CQL异常开头部分
     */
    private static final String ERROR_HEAD = "ERROR ";
    
    /**
     * CQL异常前缀
     */
    private static final String CODE_PREFIX = "CQL-";
    
    /**
     * 前缀结束符号
     */
    private static final String ERROR_PREFIX_END = ": ";
    
    /**
     * 异常码
     */
    private String errorCode;
    
    /**
     * SQL状态码
     */
    private String sqlState;
    
    /**
     * 异常消息，待格式化
     */
    private String message;
    
    /**
     * 格式化之后的消息
     */
    private String formattedMessage;
    
    /**
     * 消息格式化工具
     */
    private MessageFormat formater;
    
    private ErrorCode(String code, String msg)
    {
        this(code, msg, null);
    }
    
    private ErrorCode(String code, String msg, String state)
    {
        this.errorCode = CODE_PREFIX + code;
        this.message = msg;
        this.sqlState = state;
        this.formater = new MessageFormat(message);
    }
    
    /**
     * 获取全部异常信息
     *
     */
    public String getFullMessage(String... reasons)
    {
        formatMessage(reasons);
        
        StringBuilder sb = new StringBuilder();
        sb.append(ERROR_HEAD);
        sb.append(errorCode);
        if (sqlState != null)
        {
            sb.append(" (" + sqlState + ") ");
        }
        
        sb.append(ERROR_PREFIX_END);
        sb.append(formattedMessage);
        return sb.toString();
    }
    
    public String getFormattedMessage()
    {
        return formattedMessage;
    }
    
    public String getSqlState()
    {
        return sqlState;
    }
    
    public String getErrorCode()
    {
        return errorCode;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public String toString()
    {
        return formattedMessage;
    }
    
    private synchronized void formatMessage(String... reasons)
    {
        this.formattedMessage = formater.format(reasons);
    }
}
