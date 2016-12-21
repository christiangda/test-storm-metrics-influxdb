package com.github.christiangda.storm.config;

/**
 * Created by christian on 12/9/16.
 */
public class ConfigKeys {
    public static final String AWS_CREDENTIALS_PROFILE = "aws.credentials.profile";

    public static final String KINESIS_STREAM_NAME = "kinesis.stream.name";
    public static final String KINESIS_STREAM_INITIAL_POSITION = "kinesis.stream.initial.position";
    public static final String KINESIS_RECORD_RETRY_LIMIT = "kinesis.record.retry.limit";
    public static final String KINESIS_REGION_NAME = "kinesis.region.name";
    public static final String KINESIS_ZOOKEEPER_END_POINTS = "kinesis.zookeeper.end.points";
    public static final String KINESIS_ZOOKEEPER_DATA_PREFIX = "kinesis.zookeeper.data.prefix";
}
