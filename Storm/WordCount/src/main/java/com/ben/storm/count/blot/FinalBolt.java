package com.ben.storm.count.blot;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.util.Map;

/**
 * @Author 001289
 * @Date 2018/3/26 23:47
 * @Description ${DESCRIPTION}
 */
public class FinalBolt extends BaseRichBolt {

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple) {

        //        最终的结果打印bolt
        System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss")+"  final bolt ");
        Map<String,Integer> counts= (Map<String, Integer>) tuple.getValue(0);
        for(Map.Entry<String,Integer> kv:counts.entrySet()){
            System.out.println(kv.getKey()+"  "+kv.getValue());
        }
        //实际应用中，最后一个阶段，大部分应该是持久化到mysql，redis，es，solr或mongodb中

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
