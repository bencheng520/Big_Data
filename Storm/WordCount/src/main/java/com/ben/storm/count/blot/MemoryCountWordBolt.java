package com.ben.storm.count.blot;

import com.ben.storm.count.util.TupleHelpers;
import org.apache.storm.Config;
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
 * @Date 2018/3/26 21:16
 * @Description ${DESCRIPTION}
 */
public class MemoryCountWordBolt extends BaseBasicBolt {

    private static final Logger logger = LoggerFactory.getLogger(MemoryCountWordBolt.class);

    Map<String,Integer> counts=new HashMap<String,Integer>();

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,5);//加入Tick时间窗口，进行统计
        return conf;
    }

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {

    }

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //时间窗口定义为10s内的统计数据，统计完毕后，发射到下一阶段的bolt进行处理
        //发射完成后retun结束，开始新一轮的时间窗口计数操作
        if(TupleHelpers.isTickTuple(tuple)){
            logger.info("count: {}",counts.size());
//            Map<String,Integer> copyMap= (Map<String, Integer>) deepCopy(counts);
            basicOutputCollector.emit(new Values(counts));//10S发射一次

//            counts.clear();
            counts=new HashMap<String,Integer>();//这个地方，不能执行clear方法，可以再new一个对象，否则下游接受的数据，有可能为空 或者深度copy也行，推荐new
            return;
        }

        //如果没到发射时间，就继续统计wordcount
     //   logger.info("线程"+Thread.currentThread().getName()+"  map 缓冲统计中......  map size："+counts.size());
        //String word=tuple.getString(0);//如果有多tick，就不用使用这种方式获取tuple里面的数据
        String word=tuple.getStringByField("word");
        Integer count=counts.get(word);
        if(count==null){
            count=0;
        }
        count++;
        counts.put(word,count);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word_map"));
    }
}
