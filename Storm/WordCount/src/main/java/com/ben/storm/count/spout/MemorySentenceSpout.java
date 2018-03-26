package com.ben.storm.count.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.util.Map;
import java.util.Random;

/**
 * @Author 001289
 * @Date 2018/3/26 21:05
 * @Description ${DESCRIPTION}
 */
public class MemorySentenceSpout extends BaseRichSpout {
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
        System.out.println("线程名："+Thread.currentThread().getName()+"  "+new DateTime().toString("yyyy-MM-dd HH:mm:ss  ")+"1s发射一次数据："+sentence);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
