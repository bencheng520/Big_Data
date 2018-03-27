package com.ben.storm.count.blot;

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
 * @Author 001289
 * @Date 2018/3/26 21:12
 * @Description ${DESCRIPTION}
 */
public class MemoryWordSplitBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(MemoryWordSplitBolt.class);

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String sentence = tuple.getString(0);
        logger.info("split-----: {}", sentence);
        //        简单的按照空格进行切分后，发射到下一阶段bolt
        for(String word:sentence.split(" ")){
            logger.info("split=====: {}", word);
            outputCollector.emit(new Values(word));//发送split
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        //声明输出的filed
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
