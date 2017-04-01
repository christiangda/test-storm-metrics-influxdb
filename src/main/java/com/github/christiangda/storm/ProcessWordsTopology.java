package com.github.christiangda.storm;

import com.github.christiangda.storm.bolts.ProcessWordsBolt;
import com.github.christiangda.storm.spouts.WordGeneratorSpout;
import com.github.christiangda.storm.utils.ProjectInformation;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.metric.LoggingMetricsConsumer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProcessWordsTopology {
    private final Logger LOG = LoggerFactory.getLogger(ProcessWordsTopology.class);
    private final String topologyName = this.getClass().getSimpleName();
    private TopologyBuilder topoBuilder = new TopologyBuilder();
    private Config topoConf = new Config();
    private LocalCluster cluster;

    private ProcessWordsTopology() {

        //************************************************************************************************************//
        //*************************** Topology Structure *************************************************************//

        this.topoBuilder.setSpout("WordGeneratorSpout", new WordGeneratorSpout(), 1);

        this.topoBuilder.setBolt(ProcessWordsBolt.NAME, new ProcessWordsBolt(), 1)
                .shuffleGrouping("WordGeneratorSpout");
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
        topoConf.registerMetricsConsumer(com.github.christiangda.storm.metrics.InfluxDBMetricsConsumer.class, 1);
        topoConf.put("metrics.reporter.name", "com.github.christiangda.storm.metrics.InfluxDBMetricsConsumer");
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
        System.out.println("Usage: " + ProcessWordsTopology.class.getName() + " <local|cluster>");
        System.exit(-1);
    }

    public static void main(String[] args) throws InvalidTopologyException, AuthorizationException, AlreadyAliveException {
        String mode = null;

        if (args.length != 1) {
            printUsageAndExit();
        } else {
            mode = args[0];
        }

        if (mode != null) {

            ProcessWordsTopology topology = new ProcessWordsTopology();

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
