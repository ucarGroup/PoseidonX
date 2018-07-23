CREATE TABLE `stream_config` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `config_name` varchar(255) DEFAULT NULL COMMENT '配置项名',
  `config_value` varchar(255) DEFAULT NULL COMMENT '配置项值',
  `config_remark` text COMMENT '配置项备注',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  KEY `idx_config_name` (`config_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='配置项表';

CREATE TABLE `stream_cql` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `cql_name` varchar(255) DEFAULT NULL COMMENT '脚本名称',
  `cql_text` text COMMENT '脚本内容',
  `cql_remark` text COMMENT '脚本备注',
  `cql_status` tinyint(3) DEFAULT NULL COMMENT '状态，是否可用',
  `user_group_id` int(11) DEFAULT NULL COMMENT '用户组ID',
  `creator_user_name` varchar(255) DEFAULT NULL COMMENT '创建人',
  `modify_user_name` varchar(255) DEFAULT NULL COMMENT '最后修改人',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  `cql_type` tinyint(4) NULL DEFAULT 0 COMMENT '脚本类型 0 jstorm 1 flink',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='cql表';

CREATE TABLE `stream_engine_version` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `version_name` varchar(255) DEFAULT NULL COMMENT '实时引擎版本名称',
  `version_type` varchar(100) DEFAULT NULL COMMENT '实时引擎版本类型 JSTORM_AM, JSTORM',
  `version_remark` varchar(500) DEFAULT NULL COMMENT '备注',
  `version_url` varchar(5000) DEFAULT NULL COMMENT 'HDFS从根目录开始的路径存储文件',
  `status` tinyint(3) DEFAULT NULL COMMENT '状态，是否可用',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  KEY `idx_version_type` (`version_type`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='引擎版本表';

CREATE TABLE `stream_flink_process` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `task_id` int(11) DEFAULT NULL COMMENT '任务ID',
  `job_id` varchar(200) DEFAULT NULL COMMENT 'JOB的ID',
  `yarn_app_id` varchar(50) DEFAULT NULL COMMENT 'app_master的ID',
  `job_log_message` text COMMENT 'job的启动日志。每行用分隔符标记分开',
  `start_time` datetime DEFAULT NULL COMMENT 'JOB开始时间',
  `task_config` text COMMENT '任务配置信息(json格式存储)，开始job时进行存储',
  `task_detail`  text COMMENT '任务详情信息(json格式存储)',
  `submit_type` int(11) DEFAULT NULL COMMENT '任务的提交类型 提交或恢复',
  `submit_result` int(11) DEFAULT NULL COMMENT '提交结果 0失败，1成功' ,
  PRIMARY KEY (`id`),
  KEY `idx_task_id` (`task_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='flink Job过程历史存储表';

CREATE TABLE `stream_jstorm_process` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `task_id` int(11) DEFAULT NULL COMMENT '任务ID',
  `top_id` varchar(200) DEFAULT NULL COMMENT '拓扑的ID',
  `yarn_app_id` varchar(50) DEFAULT NULL COMMENT 'app_master的ID',
  `top_log_message` text COMMENT '拓扑的启动日志',
  `start_time` datetime DEFAULT NULL COMMENT '开始时间',
  `yarn_app_metadata` varchar(4000) DEFAULT NULL COMMENT 'yarn环境元数据(json格式存储)',
  `task_config` text COMMENT '任务配置信息(json格式存储)',
  `task_detail`  text COMMENT '任务详情信息(json格式存储)',
  `submit_type` int(11) DEFAULT NULL COMMENT '任务的提交类型 提交或恢复',
  `submit_result` int(11) DEFAULT NULL COMMENT '提交结果 0失败，1成功',
  PRIMARY KEY (`id`),
  KEY `idx_task_id` (`task_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='jstorm top过程历史存储表';

CREATE TABLE `stream_spark_process` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `task_id` int(11) DEFAULT NULL COMMENT '任务ID',
  `yarn_app_id` varchar(50) DEFAULT NULL COMMENT 'app_master的ID',
  `log_message` text COMMENT '启动日志',
  `start_time` datetime DEFAULT NULL COMMENT '开始时间',
  `task_config` text COMMENT '任务配置信息(json格式存储)',
  `submit_result` int(11) DEFAULT NULL COMMENT '提交结果 0失败，1成功',
  PRIMARY KEY (`id`),
  KEY `idx_task_id` (`task_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='spark 任务过程历史存储表';

CREATE TABLE `stream_task` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `engine_type` tinyint(3) DEFAULT NULL COMMENT '引擎类型：Jstorm,Flink',
  `process_id` int(11) DEFAULT NULL COMMENT '最后一次成功提交任务，生成的过程ID.成功提交后回写到此字段',
  `archive_id` int(11) DEFAULT NULL COMMENT '任务文件ID',
  `archive_version_id` int(11) DEFAULT NULL COMMENT '任务文件版本ID',
  `task_name` varchar(255) DEFAULT NULL COMMENT '任务的名称',
  `task_type` int(11) DEFAULT NULL COMMENT '任务类型：监控任务',
  `task_status` tinyint(3) DEFAULT NULL COMMENT '任务状态：未开始，运行中，异常中止，终止运行',
  `audit_status` tinyint(3) DEFAULT NULL COMMENT '任务的审核状态，未审核，审核通过，审核未通过',
  `audit_time` datetime DEFAULT NULL COMMENT '审核时间',
  `audit_user_name` varchar(255) DEFAULT NULL COMMENT '审核人',
  `remark` varchar(255) DEFAULT NULL COMMENT '备注',
  `task_config` text COMMENT '任务的配置信息，每个引擎类型填写的内容不同',
  `error_info` text COMMENT '任务执行过程中最近一次的异常信息',
  `task_start_time` datetime DEFAULT NULL COMMENT '任务开始时间',
  `task_stop_time` datetime DEFAULT NULL COMMENT '任务结束时间（重新开始时被清空）',
  `user_group_id` int(11) DEFAULT NULL COMMENT '用户组ID',
  `delete_status` tinyint(3) DEFAULT NULL COMMENT '是否删除状态',
  `is_cql` tinyint(3) DEFAULT NULL COMMENT '是否是 cql 0 否 1是',
  `creator_user_name` varchar(255) DEFAULT NULL COMMENT '创建人',
  `modify_user_name` varchar(255) DEFAULT NULL COMMENT '最后修改人',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  KEY `idx_engine_type` (`engine_type`,`delete_status`) USING BTREE,
  KEY `idx_task_name` (`task_name`,`delete_status`) USING BTREE,
  KEY `idx_task_status` (`task_status`,`delete_status`) USING BTREE,
  KEY `idx_audit_status` (`audit_status`,`delete_status`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务表';

CREATE TABLE `stream_task_archive` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `archive_name` varchar(255) DEFAULT NULL COMMENT '任务文件名字',
  `archive_remark` varchar(500) DEFAULT NULL COMMENT '任务文件备注',
  `status` tinyint(3) DEFAULT NULL COMMENT '状态，是否可用',
  `user_group_id` int(11) DEFAULT NULL COMMENT '用户组ID',
  `create_user` varchar(255) DEFAULT NULL COMMENT '创建人',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务文件表';

CREATE TABLE `stream_task_archive_version` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `archive_version_url` text COMMENT '任务的版本文件在hdfs的存储路径',
  `archive_id` int(11) DEFAULT NULL COMMENT '任务文件ID',
  `archive_version_remark` text COMMENT '备注',
  `create_user` varchar(255) DEFAULT NULL COMMENT '创建人',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  KEY `idx_archive_id` (`archive_id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='任务文件版本表';

CREATE TABLE `stream_user` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `user_name` varchar(255) DEFAULT NULL COMMENT '用户名',
  `password` varchar(255) DEFAULT NULL COMMENT '密码',
  `mobile` varchar(255) DEFAULT NULL COMMENT '电话',
  `user_role` tinyint(3) DEFAULT NULL COMMENT '角色',
  `user_status` tinyint(3) DEFAULT NULL COMMENT '状态，是否在用',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`),
  KEY `idx_user_name` (`user_name`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户表';

CREATE TABLE `stream_user_group` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `name` varchar(255) DEFAULT NULL COMMENT '用户组名',
  `members` text COMMENT '用户组成员',
  `create_time` datetime DEFAULT NULL COMMENT '创建时间',
  `modify_time` datetime DEFAULT NULL COMMENT '修改时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户组表';

CREATE TABLE `stream_user_login_history` (
  `id` int(11) unsigned NOT NULL AUTO_INCREMENT COMMENT '主键',
  `user_name` varchar(255) DEFAULT NULL COMMENT '用户',
  `user_role` varchar(255) DEFAULT NULL COMMENT '角色',
  `login_ip` varchar(255) DEFAULT NULL COMMENT '登录ID',
  `login_time` datetime DEFAULT NULL COMMENT '登录时间',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='用户登录历史表';

INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('1', 'JSTORM_HOME', '/usr/local/jstorm/jstorm-2.2.1/', '本地的jstorm路径,用于提交任务时候使用', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('2', 'JSTORM_NIMBUS_MEM', '2048', 'Nimbus Container的内存数（默认1024M）', '2018-02-07 10:59:58', '2018-06-14 15:00:31');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('3', 'LOCAL_PROJECT_ITEM_DIR', '/usr/local/tmpjar/', '本地项目包上传路径', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('4', 'JSTORM_TASK_LOG_PREFIX', '/data/log/hadoop/', '服务器jstorm任务日志前缀', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('5', 'ZK_HOST', 'localhost', '实时引擎用到的zk地址', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('6', 'ZK_PORT', '2181', '实时引擎用到的zk端口', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('7', 'FLINK_HOME', '/usr/local/flink-1.3.2', '本地的 FLINK 安装路径', '2018-02-22 16:04:56', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('8', 'JSTORM_CLUSTER_RECOVERY_FLAG', '0', '当Nimbus无心跳超时，是否强制恢复(默认为0，1为打开)', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('9', 'JSTORM_AUTO_RECOVERY_RETRY', '3', 'JSTORM 自动恢复的重试次数(默认为3次)', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('10', 'JSTORM_TOP_START_TIMEOUT', '150', 'JSTORM TOP任务启动的最大等待时间。超过此时间认为启动失败 (秒) 默认为1分半', '2018-02-01 11:58:41', '2018-05-29 18:55:24');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('11', 'CQL_ENGINE_PROJECT_NAME', 'streamCQL', 'CQL引擎的任务文件名称', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('12', 'FLINK_JOB_MANGER_MEM', '2048', 'JOB_MANGER 的内存数（默认2048M）', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('13', 'FLINK_JOB_START_TIMEOUT', '60', 'FLINK_JOB 启动的最大等待时间。超过此时间认为启动失败 (秒) 默认为1分钟,注意这个时间不是yarn-session的最大启动等待时间，yarn-session的最大启动等待时间为90秒。', '2018-02-01 11:58:41', '2018-06-14 14:59:24');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('14', 'HBASE_ZK_ROOT', '/hbase_0.98', 'hbase的zk root', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('15', 'HBASE_ZK_PORT', '2181', 'hbase的zk port', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('16', 'HBASE_ZK_HOST', 'localhost', 'hbase的zk host', '2018-02-01 11:58:41', '2018-03-23 16:12:48');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('17', 'EMAIL_ALARM_OPEN', '1', '邮箱告警总开关', '2018-02-01 11:58:41', '2018-02-01 11:58:41');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('18', 'PHONE_ALARM_OPEN', '1', '手机告警总开关', '2018-02-01 11:58:41', '2018-05-07 20:09:57');
INSERT INTO `stream_config` (id,config_name,config_value,config_remark,create_time,modify_time) VALUES ('19', 'FLINK_AUTO_RECOVERY_RETRY', '3', 'FLINK 自动恢复的重试次数', '2018-05-23 14:31:30', null);

INSERT INTO `stream_user` (id,user_name,password,mobile,user_role,user_status,create_time,modify_time) VALUES ('1', 'admin@ucarinc.com', '*****', '', 0, 0, '2018-01-31 09:08:20', '2018-01-31 20:40:25');