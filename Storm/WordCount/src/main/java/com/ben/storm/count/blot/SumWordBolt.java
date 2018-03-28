package com.ben.storm.count.blot;

import com.ben.storm.count.util.TupleHelpers;
import org.apache.storm.Config;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author 001289
 * @Date 2018/3/26 23:30
 * @Description ${DESCRIPTION}
 */
public class SumWordBolt extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(SumWordBolt.class);

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);//tick时间窗口3秒后，发射到下一阶段的bolt，仅为测试用
        return conf;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    Map<String,Integer> counts=new HashMap<String,Integer>();

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //tick时间窗口3秒后，发射到下一阶段的bolt，仅为测试用，故多加了这个bolt逻辑
        if(TupleHelpers.isTickTuple(tuple)){
            logger.info("sum:{}", JSON.toString(counts));
            basicOutputCollector.emit(new Values(counts));
            return;
        }

        counts= (Map<String, Integer>) tuple.getValueByField("word_map");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("final_result"));
    }
}
