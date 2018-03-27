package com.ben.storm.count.spout;


import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

/**
 * @Author 001289
 * @Date 2018/3/26 21:05
 * @Description ${DESCRIPTION}
 */
public class MemorySentenceSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(MemorySentenceSpout.class);

    SpoutOutputCollector collector;
    String [] sentences=null;
    Random random;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.random=new Random();//
        this.sentences = new String[]{
                "this is a big data demo",
                "this is a big data storm demo",
                "how are you!"
        };
    }

    public void nextTuple() {
        Utils.sleep(1000);
        //获取数据
        String sentence=sentences[random.nextInt(sentences.length)];
        logger.info("spout: {}", sentence);
        collector.emit(new Values(sentence));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
