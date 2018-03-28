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
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

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
    private ConcurrentHashMap<UUID, Values> _pending;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        this.random=new Random();//
        this.sentences = new String[]{
                "this is a big data demo",
                "this is a big data storm demo",
                "how are you!"
        };
        _pending = new ConcurrentHashMap<UUID, Values>();
    }

    public void nextTuple() {
        Utils.sleep(1000);
        //获取数据
        String sentence=sentences[random.nextInt(sentences.length)];
        logger.info("spout: {}", sentence);
        Values v = new Values(sentence);
        UUID msgId = UUID.randomUUID();
        this._pending.put(msgId, v);//spout对发射的tuple进行缓存
        collector.emit(v, msgId);//发射tuple时，添加msgId
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }

    @Override
    public void ack(Object msgId) {
        this._pending.remove(msgId);//对于成功处理的tuple从缓存队列中删除
    }

    @Override
    public void fail(Object msgId) {
        this.collector.emit(this._pending.get(msgId), msgId);//当消息处理失败了，重新发射，当然也可以做其他的逻辑处理
    }
}
