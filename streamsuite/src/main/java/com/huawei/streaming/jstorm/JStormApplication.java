/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.huawei.streaming.jstorm;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.alibaba.fastjson.JSONObject;
import com.backtype.storm.StormSubmitterForUCar;
import com.huawei.streaming.application.ApplicationResults;
import com.huawei.streaming.application.DistributeType;
import com.huawei.streaming.application.GroupInfo;
import com.huawei.streaming.config.StreamingConfig;
import com.huawei.streaming.cql.DriverContext;
import com.huawei.streaming.exception.ErrorCode;
import com.huawei.streaming.exception.StreamingException;
import com.ucar.streamsuite.common.constant.StreamContant;
import com.ucar.streamsuite.common.util.CreateToJar;
import com.ucar.streamsuite.engine.constants.EngineContant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.huawei.streaming.application.Application;
import com.huawei.streaming.operator.FunctionOperator;
import com.huawei.streaming.operator.FunctionStreamOperator;
import com.huawei.streaming.operator.IRichOperator;
import com.huawei.streaming.storm.*;

import backtype.storm.LocalCluster;
import backtype.storm.generated.*;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.NimbusClient;
import shade.storm.org.apache.thrift.TException;

/**
 * Storm应用管理
 * <功能详细描述>
 *
 */
public class JStormApplication extends Application {

	private static final Logger LOG = LoggerFactory.getLogger(JStormApplication.class);

	private TopologyBuilder builder;

	private StormConf stormConf;

	private StreamingSecurity streamingSecurity = null;

	private boolean ackable;

	private DriverContext driverContext;

	/**
	 * <默认构造函数>
	 *
	 */
	public JStormApplication(StreamingConfig config, String appName, DriverContext driverContext) throws StreamingException {


		super(appName, config);

        this.driverContext = driverContext;
		builder = new TopologyBuilder();
		stormConf = new StormConf(config,this.driverContext.getCqlClient().getCustomizedConfigurationMap());
		streamingSecurity = SecurityFactory.createSecurity(config);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public void launch() throws StreamingException {
		//local模式下不去检查应用程序是否已经存在
		//只在远程模式下检查
		String tmp = "";
		ackable = (tmp = System.getProperty("ack",null)) == null ? false : Boolean.parseBoolean(tmp);
		if (!stormConf.isSubmitLocal() && isApplicationExists()) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_EXISTS, getAppName());
			LOG.error("Application already exists.",exception);
			throw exception;
		}

		if (stormConf.isSubmitLocal()) {
			localLaunch();
		} else {
		    try {
                remoteLaunch();
            }catch(Exception e){
                StreamingException exception = new StreamingException("submit topology error", e);
                LOG.error("submit topology error.",exception);
                throw exception;
            }
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public boolean isApplicationExists() throws StreamingException {
		LOG.debug("Start to check is application exists.");
		Map<String, Object> conf = stormConf.createStormConf();
		NimbusClient client = null;
		try {
			streamingSecurity.initSecurity();
			client = NimbusClient.getConfiguredClient(conf);
			ClusterSummary clusterInfo = client.getClient().getClusterInfo();
			List<TopologySummary> list = clusterInfo.get_topologies();
			for (TopologySummary ts : list) {
				if (ts.get_name().equals(getAppName())) {
					return true;
				}
			}
			return false;
		} catch (AuthorizationException e) {
			StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
			LOG.error("No Authorization.");
			throw exception;
		} catch (TException e) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
			LOG.error("Failed to connect to application server for thrift error.",e);
			throw exception;
		} catch (Exception e) {
			StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
			LOG.error("Failed to connect to application server.",e);
			throw exception;
		} finally {
			try {
				streamingSecurity.destroySecurity();
			} catch (StreamingException e) {
				LOG.warn("Destory Security error.");
			}
			if (client != null) {
				client.close();
			}
		}

	}

	/**
	 * {@inheritDoc}
	 */
	@Override public ApplicationResults getApplications() throws StreamingException {
		NimbusClient client = null;
		try {
			streamingSecurity.initSecurity();
			client = NimbusClient.getConfiguredClient(stormConf.createStormConf());
			ClusterSummary clusterSummary = client.getClient().getClusterInfo();
			List<TopologySummary> topologies = clusterSummary.get_topologies();
			return new JStormApplicationResults(topologies);
		} catch (AuthorizationException e) {
			StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
			LOG.error("No Authorization.");
			throw exception;
		} catch (TException e) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
			LOG.error("Failed to connect to application server for thrift error.",e);
			throw exception;
		} catch (Exception e) {
			StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
			LOG.error("Failed to connect to application server.",e);
			throw exception;
		} finally {
			try {
				streamingSecurity.destroySecurity();
			} catch (StreamingException e) {
				LOG.warn("Destory Security error.");
			}
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public void killApplication() throws StreamingException {
		NimbusClient client = null;
		try {
			streamingSecurity.initSecurity();
			client = NimbusClient.getConfiguredClient(stormConf.createStormConf());
			client.getClient().killTopologyWithOpts(getAppName(), createKillOptions());
			int maxRetryTime = stormConf.getKillApplicationOverTime();
			while (true) {
				if (maxRetryTime <= 0) {
					StreamingException exception = new StreamingException(ErrorCode.PLATFORM_KILL_OVERTIME);
					LOG.error("Kill application timeout.", exception);
					throw exception;
				}
				ClusterSummary clusterInfo = client.getClient().getClusterInfo();
				List<TopologySummary> list = clusterInfo.get_topologies();
				if (isApplicationExistsAfterKilled(list)) {
					maxRetryTime--;
					sleepSeconds(1);
				} else {
					break;
				}
			}
		} catch (NotAliveException e) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_NOT_EXISTS, getAppName());
			LOG.error("Application {} not exists.", getAppName());
			throw exception;
		} catch (AuthorizationException e) {
			StreamingException exception = new StreamingException(ErrorCode.SECURITY_AUTHORIZATION_ERROR);
			LOG.error("No Authorization.");
			throw exception;
		} catch (TException e) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NIMBUS_SERVER_EXCEPTION);
			LOG.error("Failed to connect to application server for thrift error.",e);
			throw exception;
		} catch (Exception e) {
			//为了兼容社区版本和HA版本，防止引入HA相关异常
			if (e instanceof StreamingException) {
				throw (StreamingException) e;
			}

			StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
			LOG.error("Failed to connect to application server.",e);
			throw exception;
		} finally {
			try {
				streamingSecurity.destroySecurity();
			} catch (StreamingException e) {
				LOG.warn("Destory Security error.");
			}
			if (client != null) {
				client.close();
			}
		}
	}

	/**
	 * {@inheritDoc}
	 */
	@Override public void setUserPackagedJar(String userJar) {
		LOG.info("reset submit jar to {}", userJar);
		stormConf.setDefaultJarPath(userJar);
	}

	@Override
	public void deactiveApplication() throws StreamingException {
		throw new StreamingException("deactive is not supportted in jstorm application");
	}

	@Override
	public void activeApplication() throws StreamingException {
		throw new StreamingException("activeApplication is not supportted in jstorm application");
	}

	@Override
	public void rebalanceApplication(int workerNum) throws StreamingException {
		throw new StreamingException("rebalance is not supportted in jstorm application");
	}

	private boolean isApplicationExistsAfterKilled(List<TopologySummary> list) {
		boolean isFound = false;
		for (TopologySummary ts : list) {
			if (ts.get_name().equals(getAppName())) {
				isFound = true;
				break;
			}
		}
		return isFound;
	}

	private KillOptions createKillOptions() {
		KillOptions kop = new KillOptions();
		kop.set_wait_secs(stormConf.getKillWaitingSeconds());
		return kop;
	}

	private void sleepSeconds(int seconds) {
		try {
			TimeUnit.SECONDS.sleep(seconds);
		} catch (InterruptedException e) {
			LOG.error("Interrupted while thread sleep.");
		}
	}

	/**
	 * 创建topology，远程提交拓扑时使用
	 *
	 */
	private void createTopology() throws StreamingException {
		createSpouts();
		createBolts();
	}

	private void createSpouts() throws StreamingException {
		List<? extends IRichOperator> sources = getInputStreams();
		checkInputStreams(sources);
		for (IRichOperator input : sources) {
			StormSpout spout = new StormSpout();
			spout.setOperator(input);
			builder.setSpout(input.getOperatorId(), spout, input.getParallelNumber());
		}
	}

	private void checkInputStreams(List<? extends IRichOperator> operators) throws StreamingException {
		if (null == operators || operators.isEmpty()) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_NO_INPUT_OPERATOR);
			LOG.error("No input operator.");
			throw exception;
		}
	}

	private void createBolts() throws StreamingException {
		List<IRichOperator> orderedFunOp = genFunctionOpsOrder();
		if (orderedFunOp == null || orderedFunOp.isEmpty()) {
			LOG.debug("Topology don't have any function operator");
			return;
		}

		for (IRichOperator operator : orderedFunOp) {
			setOperatorGrouping(operator);
		}
	}

	private void setOperatorGrouping(IRichOperator operator) throws StreamingException {
		BoltDeclarer bolt = createBoltDeclarer(operator);
		List<String> streams = operator.getInputStream();
		if (streams == null) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
			LOG.error("The operator input streaming is null.");
			throw exception;
		}

		for (String strname : operator.getInputStream()) {
			GroupInfo groupInfo = operator.getGroupInfo().get(strname);
			setBoltGrouping(bolt, strname, groupInfo);
		}
	}

	private void setBoltGrouping(BoltDeclarer bolt, String strname, GroupInfo groupInfo) throws StreamingException {
		if (null == groupInfo) {
			setDefaultBoltGrouping(bolt, strname);
			return;
		}

		DistributeType distribute = groupInfo.getDitributeType();
		switch (distribute) {
		case FIELDS:
			Fields fields = new Fields(groupInfo.getFields());
			IRichOperator operator = getOperatorByOutputStreamName(strname);
			if (operator == null) {
				StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
				LOG.error("Can't find opertor by stream name : {} .", strname, exception);
				throw exception;
			}
			bolt.fieldsGrouping(operator.getOperatorId(), strname, fields);
			break;
		case GLOBAL:
			break;
		case LOCALORSHUFFLE:
			break;
		case ALL:
			break;
		case DIRECT:
			break;
		case CUSTOM:
			break;
		case SHUFFLE:
		case NONE:
		default:
			setDefaultBoltGrouping(bolt, strname);
		}
	}

	private void setDefaultBoltGrouping(BoltDeclarer bolt, String strname) throws StreamingException {
		IRichOperator operator = getOperatorByOutputStreamName(strname);
		if (operator == null) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
			LOG.error("Can't find opertor by stream name : {} .", strname, exception);
			throw exception;
		}
		bolt.shuffleGrouping(operator.getOperatorId(), strname);
	}

	private BoltDeclarer createBoltDeclarer(IRichOperator operator) {
		IRichBolt bolt;
		if ((operator instanceof FunctionOperator) || (operator instanceof FunctionStreamOperator)) {
			bolt = createStormBolt(operator);
		} else {
			bolt = createOutputStormBolt(operator);
		}
		return builder.setBolt(operator.getOperatorId(), bolt, operator.getParallelNumber());
	}

	private IRichBolt createOutputStormBolt(IRichOperator f) {
		StormOutputBolt outputbolt = new StormOutputBolt();
		outputbolt.setOperator(f);
		return outputbolt;
	}

	private IRichBolt createStormBolt(IRichOperator f) {
		StormBolt stormbolt = new StormBolt();
		stormbolt.setOperator(f);
		return stormbolt;
	}

	private void localLaunch() throws StreamingException {

		Map<String, Object> stormconf = stormConf.createStormConf();
		createTopology();
		StormTopology topology = builder.createTopology();
		if (stormConf.isTestModel()) {
			return;
		}
		stormConf.setStormJar(null);
		LocalCluster cluster = null;
		try {
			streamingSecurity.initSecurity();
			cluster = new LocalCluster();
            stormconf.put("storm.local.dir", JSONObject.parseObject(cluster.getLocalClusterMap().getNimbus().getNimbusConf()).get("storm.local.dir"));

            cluster.submitTopology(getAppName(), stormconf, topology);
			long aliveTime = stormConf.getLocalTaskAliveTime();
			sleepMilliSeconds(aliveTime);
		} catch (TException e) {
            e.printStackTrace();
        } finally {
			try {
				streamingSecurity.destroySecurity();
			} catch (StreamingException e) {
				LOG.warn("Destory Security error.");
			}

			if (cluster != null) {
				cluster.shutdown();
			}
		}
	}

	private void remoteLaunch() throws StreamingException, IOException, URISyntaxException {
        Map<String, Object> conf = stormConf.createStormConf();

        //创建APP拓扑
        createTopology();

        String alljarpath = String.valueOf(conf.get(EngineContant.RTCP_CQL_JAR_PATH_NAME));

        stormConf.setStormJar(alljarpath);

        StormTopology topology = builder.createTopology();
        submitTopology(conf, topology);

		CreateToJar.deleteFileinPath(alljarpath);
	}

	private void submitTopology(Map<String, Object> conf, StormTopology topology) throws StreamingException {
		try {
			streamingSecurity.initSecurity();
            StormSubmitterForUCar.submitTopology(getAppName(), conf, topology);
		} catch (AlreadyAliveException e) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_APP_EXISTS, getAppName());
			LOG.error("Application already exists.");
			throw exception;
		} catch (InvalidTopologyException e) {
			StreamingException exception = new StreamingException(ErrorCode.PLATFORM_INVALID_TOPOLOGY);
			LOG.error("The submit topology is invalid.");
			throw exception;
		} catch (Exception e) {
			//为了兼容社区版本和HA版本，防止引入HA相关异常
			if (e instanceof StreamingException) {
				throw (StreamingException) e;
			}

			StreamingException exception = new StreamingException(ErrorCode.UNKNOWN_SERVER_COMMON_ERROR);
			LOG.error("Failed to connect to application server.",e);
			throw exception;
		} finally {
			try {
				streamingSecurity.destroySecurity();
			} catch (StreamingException e) {
				LOG.warn("Destory Security error.");
			}
		}
	}

	private void sleepMilliSeconds(long milliSeconds) {
		try {
			TimeUnit.MILLISECONDS.sleep(milliSeconds);
		} catch (InterruptedException e) {
			LOG.error("Interrupt while thread sleep.");
		}
	}


}
