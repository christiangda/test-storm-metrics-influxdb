package com.github.christiangda.storm.config;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.kinesis.stormspout.InitialPositionInStream;
import org.omg.CosNaming.NamingContextPackage.NotFound;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by christian on 12/9/16.
 */
public class ParseConfig {
    private final Logger LOG = LoggerFactory.getLogger(ParseConfig.class);
    private Map<String, Object> conf = new HashMap<>();

    /**
     * @param yamlFile
     * @throws NotFound
     */
    public ParseConfig(String yamlFile) throws NotFound {
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(yamlFile);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Map<String, Object> confFile = null;
        try {
            Yaml yaml = new Yaml();
            confFile = (Map<String, Object>) yaml.load(fis);
            LOG.info("Parsing configuration file");
        } catch (YAMLException e) {
            LOG.error("Error parsing YAML file for configuration : " + "\n" + e.getMessage());
            System.err.println("Error parsing YAML file for configuration : ");
            System.exit(1);
        }

        addConfigValueFromFile(ConfigKeys.AWS_CREDENTIALS_PROFILE, confFile);
        addConfigValueFromFile(ConfigKeys.KINESIS_STREAM_NAME, confFile);

        // KINESIS_STREAM_INITIAL_POSITION
        if (confFile.containsKey(ConfigKeys.KINESIS_STREAM_INITIAL_POSITION)) {
            LOG.info("set " + ConfigKeys.KINESIS_STREAM_INITIAL_POSITION + " = " + confFile.get(ConfigKeys.KINESIS_STREAM_INITIAL_POSITION));

            String s = confFile.get(ConfigKeys.KINESIS_STREAM_INITIAL_POSITION).toString();
            switch (s) {
                case "LATEST":
                    this.conf.put(ConfigKeys.KINESIS_STREAM_INITIAL_POSITION, InitialPositionInStream.LATEST);
                    break;
                case "TRIM_HORIZON":
                    this.conf.put(ConfigKeys.KINESIS_STREAM_INITIAL_POSITION, InitialPositionInStream.TRIM_HORIZON);
                    break;
                default:
                    System.exit(1);
            }
        }

        addConfigValueFromFile(ConfigKeys.KINESIS_RECORD_RETRY_LIMIT, confFile);

        // KINESIS_REGION_NAME
        if (confFile.containsKey(ConfigKeys.KINESIS_REGION_NAME)) {
            LOG.info("set " + ConfigKeys.KINESIS_REGION_NAME + " = " + confFile.get(ConfigKeys.KINESIS_REGION_NAME));

            String s = confFile.get(ConfigKeys.KINESIS_REGION_NAME).toString();
            switch (s) {
                case "us-west-2":
                    this.conf.put(ConfigKeys.KINESIS_REGION_NAME, Regions.US_WEST_2);
                    break;
                case "us-west-1":
                    this.conf.put(ConfigKeys.KINESIS_REGION_NAME, Regions.US_WEST_1);
                    break;
                case "us-east-1":
                    this.conf.put(ConfigKeys.KINESIS_REGION_NAME, Regions.US_EAST_1);
                    break;
            }
        }

        addConfigValueFromFile(ConfigKeys.KINESIS_ZOOKEEPER_DATA_PREFIX, confFile);
        addConfigValueFromFile(ConfigKeys.KINESIS_ZOOKEEPER_END_POINTS, confFile);
    }

    /**
     * @param key
     * @param confFile
     */
    private void addConfigValueFromFile(String key, Map<String, Object> confFile) {
        if (confFile.containsKey(key)) {
            LOG.info("set " + key + " = " + confFile.get(key));
            this.conf.put(key, confFile.get(key));
        }
    }

    /**
     * @return conf Map<String, Object>
     */
    public Map<String, Object> getConf() {
        return conf;
    }
}
