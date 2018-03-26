package com.ben.storm.count.blot;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.ben.storm.count.util.TupleHelpers;
import org.apache.storm.shade.org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author 001289
 * @Date 2018/3/26 23:30
 * @Description ${DESCRIPTION}
 */
public class SumWordBlot extends BaseRichBolt {

    private  OutputCollector outputCollector;

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 3);//tick时间窗口3秒后，发射到下一阶段的bolt，仅为测试用
        return conf;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    Map<String,Integer> counts=new HashMap<String,Integer>();

    public void execute(Tuple tuple) {
        //tick时间窗口3秒后，发射到下一阶段的bolt，仅为测试用，故多加了这个bolt逻辑
        if(TupleHelpers.isTickTuple(tuple)){
            System.out.println(new DateTime().toString("yyyy-MM-dd HH:mm:ss")+"  showbolt间隔  应该是 3 秒后 ");
//        System.out.println("what: "+tuple.getValue(0)+"  "+tuple.getFields().toList());
            outputCollector.emit(new Values(counts));
            return;
        }

        counts= (Map<String, Integer>) tuple.getValueByField("word_map");
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        outputFieldsDeclarer.declare(new Fields("final_result"));
    }
}
