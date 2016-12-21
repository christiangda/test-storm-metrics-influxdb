package com.github.christiangda.storm;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;
import com.amazonaws.services.kinesis.stormspout.KinesisSpout;
import com.amazonaws.services.kinesis.stormspout.KinesisSpoutConfig;
import com.github.christiangda.storm.bolts.ProcessEventBolt;
import com.github.christiangda.storm.config.ConfigKeys;
import com.github.christiangda.storm.config.ParseConfig;
import com.github.christiangda.storm.spouts.MainKinesisRecordScheme;
import com.github.christiangda.storm.utils.CustomCredentialsProviderChain;
import com.github.christiangda.storm.utils.ProjectInformation;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.shade.org.apache.zookeeper.KeeperException;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.omg.CosNaming.NamingContextPackage.NotFound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Created by christian on 12/9/16.
 */
public class ProcessEvents {
    private final Logger LOG = LoggerFactory.getLogger(ProcessEvents.class);
    private final String topologyName = this.getClass().getSimpleName();
    private TopologyBuilder topoBuilder = new TopologyBuilder();
    private Config topoConf = new Config();
    private LocalCluster cluster;

    private ProcessEvents(Map<String, Object> globalConf) {

        //Set the access to Kinesis Stream
        CustomCredentialsProviderChain awsCredentials = new CustomCredentialsProviderChain(
                globalConf.get(ConfigKeys.AWS_CREDENTIALS_PROFILE).toString()
        );

/*        LOG.debug("Credentials {}, {}",
                awsCredentials.getCredentials().getAWSAccessKeyId(),
                awsCredentials.getCredentials().getAWSSecretKey());*/

        //
        KinesisSpoutConfig kinesisConf = new KinesisSpoutConfig(
                globalConf.get(ConfigKeys.KINESIS_STREAM_NAME).toString(),
                globalConf.get(ConfigKeys.KINESIS_ZOOKEEPER_END_POINTS).toString()
        )
                .withZookeeperPrefix((String) globalConf.get(ConfigKeys.KINESIS_ZOOKEEPER_DATA_PREFIX))
                .withKinesisRecordScheme(new MainKinesisRecordScheme())
                .withInitialPositionInStream((InitialPositionInStream) globalConf.get(ConfigKeys.KINESIS_STREAM_INITIAL_POSITION))
                .withRecordRetryLimit((Integer) globalConf.get(ConfigKeys.KINESIS_RECORD_RETRY_LIMIT))
                .withRegion((Regions) globalConf.get(ConfigKeys.KINESIS_REGION_NAME));

        //
        final KinesisSpout GetEventsSpout = new KinesisSpout(kinesisConf, awsCredentials, new ClientConfiguration());

        //**************************************************************************//
        //*************************** Topology Structure ***************************//
        this.topoBuilder.setSpout("GetEventsSpout", GetEventsSpout, 1);
        this.topoBuilder.setBolt(ProcessEventBolt.NAME, new ProcessEventBolt(), 1).shuffleGrouping("GetEventsSpout");
    }

    private void runLocal(int runTime) {
        LOG.info("Project Version = {}", ProjectInformation.getVersion());
        LOG.info("Project GroupId = {}", ProjectInformation.getgroupId());
        LOG.info("Project ArtifactId = {}", ProjectInformation.getArtifactId());

        LOG.info("-- Running storm topology in local mode --");
        LOG.info("-- Submitting topology " + topologyName + " to local cluster --");

        //topoConf.setDebug(true);
        topoConf.setNumWorkers(2);
        topoConf.registerMetricsConsumer(LoggingMetricsConsumer.class, 1);
        topoConf.registerMetricsConsumer(com.txrlabs.storm.metrics.InfluxDBMetricsConsumer.class, 1);
        topoConf.put("metrics.reporter.name", "com.txrlabs.storm.metrics.InfluxDBMetricsConsumer");
        topoConf.put("metrics.influxdb.url", "http://localhost:8086");
        topoConf.put("metrics.influxdb.username", "test");
        topoConf.put("metrics.influxdb.password", "test");
        topoConf.put("metrics.influxdb.database", "storm-metrics");
        topoConf.put("metrics.influxdb.measurement.prefix", "test-");
        topoConf.put("metrics.influxdb.enable.gzip", true);

        cluster = new LocalCluster();
        cluster.submitTopology(topologyName, topoConf, topoBuilder.createTopology());
        if (runTime > 0) {
            Utils.sleep(runTime);
            shutDownLocal();
        }
    }

    private void shutDownLocal() {
        if (cluster != null) {
            cluster.killTopology(topologyName);
            cluster.shutdown();
        }
    }

    private void runCluster() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
        LOG.debug("-- Running storm topology in cluster mode --");
        LOG.info("-- Submitting topology " + topologyName + " to cluster --");

        topoConf.setNumWorkers(1);
        topoConf.setMaxSpoutPending(1000);

        StormSubmitter.submitTopology(topologyName, topoConf, topoBuilder.createTopology());
    }

    private static void printUsageAndExit() {
        System.out.println("Usage: " + ProcessEvents.class.getName() + " <yamlFileConfig> <local|cluster>");
        System.exit(-1);
    }

    public static void main(String[] args) throws IllegalArgumentException, KeeperException, InterruptedException,
            AlreadyAliveException, InvalidTopologyException, IOException, NotFound, AuthorizationException {
        String fileConfig = null;
        String mode = null;

        if (args.length != 2) {
            printUsageAndExit();
        } else {
            fileConfig = args[0];
            mode = args[1];
        }

        // Global Configuration for Topology
        ParseConfig config = new ParseConfig(fileConfig);

        if (mode != null) {

            ProcessEvents topology = new ProcessEvents(config.getConf());

            if (mode.equals("local")) {
                topology.runLocal(1000000);
            } else if (mode.equals("cluster")) {
                topology.runCluster();
            }
        } else {
            printUsageAndExit();
        }

    }
}
