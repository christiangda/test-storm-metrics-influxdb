package com.github.christiangda.storm.spouts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

/**
 * Created by christian on 1/4/17.
 */
public class WordGeneratorSpout extends BaseRichSpout {
    private static final Logger LOG = LoggerFactory.getLogger(WordGeneratorSpout.class);

    public static final String OUTPUT_FIELD = "word";
    public static final String NAME = "WordGeneratorSpout";

    SpoutOutputCollector collector;
    Random random;

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.random = new Random();
    }

    @Override
    public void nextTuple() {
        Utils.sleep(100);

        final String[] words = new String[]{"one", "two", "three", "four", "five", "unknown"};
        final String word = words[this.random.nextInt(words.length)];

        LOG.debug("{}: Emitting tuple: {}", this.getClass().getSimpleName(), word);
        this.collector.emit(new Values(word));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(OUTPUT_FIELD));
    }
}
