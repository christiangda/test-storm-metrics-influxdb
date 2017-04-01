package com.github.christiangda.storm.bolts;

import com.github.christiangda.storm.metrics.SuccessRateMetric;
import com.github.christiangda.storm.metrics.TupleRateMetric;
import com.github.christiangda.storm.spouts.WordGeneratorSpout;
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

import java.util.Map;

/**
 * Created by christian on 12/9/16.
 */
public class ProcessWordsBolt extends BaseRichBolt {

    private final Logger LOG = LoggerFactory.getLogger(ProcessWordsBolt.class);
    public static final String NAME = "ProcessWordsBolt";

    private OutputCollector collector;

    private final int METRICS_WINDOW = 60;
    private transient CountMetric recivedWords;
    private transient CountMetric goodWords;
    private transient CountMetric wrongWords;
    private transient SuccessRateMetric successRate;
    private transient TupleRateMetric tupleRate;

    private transient CountMetric oneWords;
    private transient CountMetric twoWords;
    private transient CountMetric threeWords;
    private transient CountMetric fourWords;
    private transient CountMetric fiveWords;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        this.collector = collector;

        recivedWords = new CountMetric();
        context.registerMetric("recived-words", recivedWords, METRICS_WINDOW);

        successRate = new SuccessRateMetric();
        context.registerMetric("success-rate-words", successRate, METRICS_WINDOW);

        tupleRate = new TupleRateMetric(METRICS_WINDOW);
        context.registerMetric("tuple-by-second", tupleRate, METRICS_WINDOW);

        goodWords = new CountMetric();
        context.registerMetric("good-words", goodWords, METRICS_WINDOW);

        wrongWords = new CountMetric();
        context.registerMetric("wrong-words", wrongWords, METRICS_WINDOW);

        //
        oneWords = new CountMetric();
        context.registerMetric("one-words", oneWords, METRICS_WINDOW);

        twoWords = new CountMetric();
        context.registerMetric("two-words", twoWords, METRICS_WINDOW);

        threeWords = new CountMetric();
        context.registerMetric("three-words", threeWords, METRICS_WINDOW);

        fourWords = new CountMetric();
        context.registerMetric("four-words", fourWords, METRICS_WINDOW);

        fiveWords = new CountMetric();
        context.registerMetric("five-words", fiveWords, METRICS_WINDOW);
    }

    @Override
    public void execute(Tuple tuple) {

        final String word = tuple.getStringByField(WordGeneratorSpout.OUTPUT_FIELD);

        LOG.debug("{}: Received word: {}", this.getClass().getSimpleName(), word);

        recivedWords.incr();
        tupleRate.incrRecived();

        switch (word) {
            case "one":
                successRate.incrSuccess();
                goodWords.incr();
                oneWords.incr();
                break;
            case "two":
                successRate.incrSuccess();
                goodWords.incr();
                twoWords.incr();
                break;
            case "three":
                successRate.incrSuccess();
                goodWords.incr();
                threeWords.incr();
                break;
            case "four":
                successRate.incrSuccess();
                goodWords.incr();
                fourWords.incr();
                break;
            case "five":
                successRate.incrSuccess();
                goodWords.incr();
                fiveWords.incr();
                break;
            default:
                wrongWords.incr();
                successRate.incrFail();
                break;
        }

        LOG.debug("{}: Emitting word: {}", this.getClass().getSimpleName(), word);

        this.collector.emit(new Values(word));
        this.collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields(WordGeneratorSpout.OUTPUT_FIELD));
    }
}