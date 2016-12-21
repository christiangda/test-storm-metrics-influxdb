package com.github.christiangda.storm.bolts;

import com.github.christiangda.storm.metrics.SuccessRateMetric;
import com.github.christiangda.storm.metrics.TupleRateMetric;
import com.github.christiangda.storm.spouts.MainKinesisRecordScheme;
import org.apache.storm.metric.api.CountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.util.Map;

/**
 * Created by christian on 12/9/16.
 */
public class ProcessEventBolt extends BaseRichBolt {

    private final Logger LOG = LoggerFactory.getLogger(ProcessEventBolt.class);
    public static final String NAME = "ProcessEventBolt";
    public static final String FIELD_EVENT = "data";
    private transient CharsetDecoder decoder;
    private OutputCollector collector;

    private final int METRICS_WINDOW = 60;
    private transient CountMetric recivedEvents;
    private transient CountMetric processedEvents;
    private transient CountMetric failedEvents;
    private transient SuccessRateMetric successRate;
    private transient TupleRateMetric tupleRate;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        decoder = Charset.forName("UTF-8").newDecoder();

        // In case of malformed data stream (unknown characters)
        decoder.onMalformedInput(CodingErrorAction.IGNORE);
        this.collector = collector;

        recivedEvents = new CountMetric();
        context.registerMetric("recived-events", recivedEvents, METRICS_WINDOW);

        processedEvents = new CountMetric();
        context.registerMetric("processed-events", processedEvents, METRICS_WINDOW);

        failedEvents = new CountMetric();
        context.registerMetric("failed-events", failedEvents, METRICS_WINDOW);

        successRate = new SuccessRateMetric();
        context.registerMetric("success-rate-events", successRate, METRICS_WINDOW);

        tupleRate = new TupleRateMetric(METRICS_WINDOW);
        context.registerMetric("tuple-by-second", tupleRate, METRICS_WINDOW);
    }

    @Override
    public void execute(Tuple tuple) {

        LOG.debug("Executing {}", NAME);

        recivedEvents.incr();

        String partitionKey = (String) tuple.getValueByField(MainKinesisRecordScheme.FIELD_PARTITION_KEY);
        String sequenceNumber = (String) tuple.getValueByField(MainKinesisRecordScheme.FIELD_SEQUENCE_NUMBER);
        byte[] payload = (byte[]) tuple.getValueByField(MainKinesisRecordScheme.FIELD_RECORD_DATA);

        ByteBuffer buffer = ByteBuffer.wrap(payload);
        String data = null;
        tupleRate.incrRecived();
        try {
            data = decoder.decode(buffer).toString();
            processedEvents.incr();
            successRate.incrSuccess();
            this.collector.emit(new Values(data));
            this.collector.ack(tuple);
            LOG.debug("Got record: partitionKey={}, sequenceNumber={}, data={}", partitionKey, sequenceNumber, data);
        } catch (CharacterCodingException e) {
            failedEvents.incr();
            successRate.incrFail();
            this.collector.ack(tuple);
            LOG.error("Exception when decoding record: partitionKey={}, sequenceNumber={}, exception: ", partitionKey, sequenceNumber, e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(FIELD_EVENT));
    }
}