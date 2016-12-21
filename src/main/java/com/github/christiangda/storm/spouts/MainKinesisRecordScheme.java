package com.github.christiangda.storm.spouts;

import com.amazonaws.services.kinesis.model.Record;
import com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme;
import org.apache.storm.tuple.Fields;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by christian on 12/9/16.
 */
public class MainKinesisRecordScheme implements IKinesisRecordScheme {
    private static final long serialVersionUID = 1L;
    /**
     * Name of the (partition key) value in the tuple.
     */
    public static final String FIELD_PARTITION_KEY = "partitionKey";

    /**
     * Name of the sequence number value in the tuple.
     */
    public static final String FIELD_SEQUENCE_NUMBER = "sequenceNumber";

    /**
     * Name of the Kinesis record data value in the tuple.
     */
    public static final String FIELD_RECORD_DATA = "recordData";

    /**
     * Constructor.
     */
    public MainKinesisRecordScheme() {
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme#deserialize(com.amazonaws.services.kinesis.model
     * .Record)
     */
    @Override
    public List<Object> deserialize(Record record) {
        final List<Object> l = new ArrayList<>();
        l.add(record.getPartitionKey());
        l.add(record.getSequenceNumber());
        l.add(record.getData().array());
        return l;
    }

    /*
     * (non-Javadoc)
     *
     * @see com.amazonaws.services.kinesis.stormspout.IKinesisRecordScheme#getOutputFields()
     */
    @Override
    public Fields getOutputFields() {
        return new Fields(FIELD_PARTITION_KEY, FIELD_SEQUENCE_NUMBER, FIELD_RECORD_DATA);
    }
}
