package com.ben.storm.count.blot;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @Author 001289
 * @Date 2018/3/26 23:47
 * @Description ${DESCRIPTION}
 */
public class FinalBolt extends BaseRichBolt {

    private static final Logger logger = LoggerFactory.getLogger(FinalBolt.class);

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {

        //        最终的结果打印bolt
     //   logger.info(new DateTime().toString("yyyy-MM-dd HH:mm:ss")+"  final bolt ");
        Map<String,Integer> counts= (Map<String, Integer>) tuple.getValue(0);
        for(Map.Entry<String,Integer> kv:counts.entrySet()){
            logger.info("final:{}",kv.getKey()+"  "+kv.getValue());
        }
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
