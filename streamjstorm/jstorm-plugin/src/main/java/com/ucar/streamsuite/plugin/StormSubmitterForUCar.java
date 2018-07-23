package com.ucar.streamsuite.plugin;

import java.nio.ByteBuffer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.ConfigExtension;

import backtype.storm.Config;
import backtype.storm.GenericOptionsParser;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.*;
import backtype.storm.utils.BufferFileInputStream;
import backtype.storm.utils.NimbusClient;
import backtype.storm.utils.Utils;
import shade.storm.org.apache.thrift.TException;

/**
 * 微调 jstorm 2.2.1 的 StormSubmitter
 * 原版的  无法在一个进程里面连续提交任务
 * Created on 2017/2/27 下午5:21:41
 *
 */
public class StormSubmitterForUCar extends StormSubmitter {

    public static Logger LOG = LoggerFactory.getLogger(StormSubmitterForUCar.class);

    private static Nimbus.Iface localNimbus = null;

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology) throws AlreadyAliveException, InvalidTopologyException {
        submitTopology(name, stormConf, topology, null);
    }

    /**
     * Submits a topology to run on the cluster. A topology runs forever or until explicitly killed.
     *
     *
     * @param name the name of the storm.
     * @param stormConf the topology-specific configuration. See {@link Config}.
     * @param topology the processing to execute.
     * @throws AlreadyAliveException if a topology with this name is already running
     * @throws InvalidTopologyException if an invalid topology was submitted
     */
    public static void submitTopology(String name, Map stormConf, StormTopology topology, SubmitOptions opts) throws AlreadyAliveException,
            InvalidTopologyException {
        if (!Utils.isValidConf(stormConf)) {
            throw new IllegalArgumentException("Storm conf is not valid. Must be json-serializable");
        }
        stormConf = new HashMap(stormConf);
        stormConf.putAll(Utils.readCommandLineOpts());
        Map conf = Utils.readStormConfig();
        conf.putAll(stormConf);
        putUserInfo(conf, stormConf);
        try {
            String serConf = Utils.to_json(stormConf);

            LOG.error("00000000000000="+serConf);
            if (localNimbus != null) {
                LOG.info("Submitting topology " + name + " in local mode");
                localNimbus.submitTopology(name, null, serConf, topology);
            } else {
                NimbusClient client = NimbusClient.getConfiguredClient(conf);
                try {
                    boolean enableDeploy = ConfigExtension.getTopologyHotDeplogyEnable(stormConf);

                    if (topologyNameExists(client, conf, name) != enableDeploy) {
                        if (enableDeploy){
                            throw new RuntimeException("Topology with name `" + name + "` not exists on cluster");
                        }else {
                            throw new RuntimeException("Topology with name `" + name + "` already exists on cluster");
                        }
                    }

                    submitJar(client, conf);
                    LOG.info("Submitting topology " + name + " in distributed mode with conf " + serConf);
                    if (opts != null) {
                        client.getClient().submitTopologyWithOpts(name, path, serConf, topology, opts);
                    } else {
                        // this is for backwards compatibility
                        client.getClient().submitTopology(name, path, serConf, topology);
                    }
                } finally {
                    client.close();
                }
            }
            LOG.info("Finished submitting topology: " + name);
        } catch (InvalidTopologyException e) {
            LOG.warn("Topology submission exception", e);
            throw e;
        } catch (AlreadyAliveException e) {
            LOG.warn("Topology already alive exception", e);
            throw e;
        } catch (TopologyAssignException e) {
            LOG.warn("Failed to assign " + e.get_msg(), e);
            throw new RuntimeException(e);
        } catch (TException e) {
            LOG.warn("Failed to assign ", e);
            throw new RuntimeException(e);
        }
    }

    public static boolean topologyNameExists(NimbusClient client, Map conf, String name) {
        try {
            client.getClient().getTopologyInfoByName(name);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    private static String submittedJar = null;
    private static String path = null;

    private synchronized static void submitJar(NimbusClient client, Map conf) {
        if (submittedJar == null) {
            try {
                LOG.info("Jar not uploaded to master yet. Submitting jar...");
                String localJar = System.getProperty("storm.jar");
                path = client.getClient().beginFileUpload();
                String[] pathCache = path.split("/");
                String uploadLocation = path + "/stormjar-" + pathCache[pathCache.length - 1] + ".jar";
                List<String> lib = (List<String>) conf.get(GenericOptionsParser.TOPOLOGY_LIB_NAME);
                Map<String, String> libPath = (Map<String, String>) conf.get(GenericOptionsParser.TOPOLOGY_LIB_PATH);
                if (lib != null && lib.size() != 0) {
                    for (String libName : lib) {
                        String jarPath = path + "/lib/" + libName;
                        client.getClient().beginLibUpload(jarPath);
                        submitJar(conf, libPath.get(libName), jarPath, client);
                    }

                } else {
                    if (localJar == null) {
                        // no lib, no client jar
                        throw new RuntimeException("No client app jar, please upload it");
                    }
                }

                if (localJar != null) {
                    submitJar(conf, localJar, uploadLocation, client);
                } else {
                    // no client jar, but with lib jar
                    client.getClient().finishFileUpload(uploadLocation);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else {
            LOG.info("Jar already uploaded to master. Not submitting jar.");
        }
    }

    public static String submitJar(Map conf, String localJar, String uploadLocation, NimbusClient client) {
        if (localJar == null) {
            throw new RuntimeException("Must submit topologies using the 'storm' client script so that StormSubmitter knows which jar to upload.");
        }

        try {

            LOG.info("Uploading topology jar " + localJar + " to assigned location: " + uploadLocation);
            int bufferSize = 512 * 1024;
            Object maxBufSizeObject = conf.get(Config.NIMBUS_THRIFT_MAX_BUFFER_SIZE);
            if (maxBufSizeObject != null) {
                bufferSize = Utils.getInt(maxBufSizeObject) / 2;
            }

            BufferFileInputStream is = new BufferFileInputStream(localJar, bufferSize);
            while (true) {
                byte[] toSubmit = is.read();
                if (toSubmit.length == 0)
                    break;
                client.getClient().uploadChunk(uploadLocation, ByteBuffer.wrap(toSubmit));
            }
            client.getClient().finishFileUpload(uploadLocation);
            LOG.info("Successfully uploaded topology jar to assigned location: " + uploadLocation);
            return uploadLocation;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {

        }
    }

    private static void putUserInfo(Map conf, Map stormConf) {
        stormConf.put("user.group", conf.get("user.group"));
        stormConf.put("user.name", conf.get("user.name"));
        stormConf.put("user.password", conf.get("user.password"));
    }


}
