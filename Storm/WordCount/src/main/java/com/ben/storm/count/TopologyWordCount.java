package com.ben.storm.count;

import com.ben.storm.count.blot.FinalBolt;
import com.ben.storm.count.blot.MemoryCountWordBolt;
import com.ben.storm.count.blot.MemoryWordSplitBolt;
import com.ben.storm.count.blot.SumWordBolt;
import com.ben.storm.count.spout.FileSentenceSpout;
import com.ben.storm.count.spout.MemorySentenceSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @Author 001289
 * @Date 2018/3/27 0:33
 * @Description ${DESCRIPTION}
 */
public class TopologyWordCount {

    private static final Logger logger = LoggerFactory.getLogger(TopologyWordCount.class);

    public static void main(String[] args) throws  Exception {
        TopologyBuilder builder=new TopologyBuilder();
        if(args.length == 3){
            if("file".equals(args[1])){
                //设置数据源
                builder.setSpout("spout",new FileSentenceSpout(),1);
            }
        }else{
            //设置数据源
            builder.setSpout("spout",new MemorySentenceSpout(),1);
        }
        //读取spout数据源的数据，进行split业务逻辑
        builder.setBolt("split",new MemoryWordSplitBolt(),1).shuffleGrouping("spout");
        //读取split后的数据，进行count (tick周期10秒)
        builder.setBolt("count",new MemoryCountWordBolt(),1).fieldsGrouping("split",new Fields("word"));
        //读取count后的数据，进行缓冲打印 （tick周期3秒，仅仅为测试tick使用，所以多加了这个bolt）
        builder.setBolt("sum",new SumWordBolt(),1).shuffleGrouping("count");
        //读取show后缓冲后的数据，进行最终的打印 （实际应用中，最后一个阶段应该为持久层）
        builder.setBolt("final",new FinalBolt(),1).allGrouping("sum");

        Config config=new Config();
        logger.info("TopologyWordCount args: {}", args);
        //集群模式
        if(args!=null&&args.length>0){
            String topologyName = args[0];
            if(args.length == 3){
                if("file".equals(args[1])){
                    String inputPath = args[2];
                    config.put("INPUT_PATH", inputPath);
                }
            }
            config.setDebug(false);
            config.setNumWorkers(2);
            StormSubmitter.submitTopology(topologyName,config,builder.createTopology());
            //单机模式
        }else{
            config.setDebug(true);
            config.setMaxTaskParallelism(1);
            LocalCluster cluster=new LocalCluster();
            cluster.submitTopology("TopologyWordCount",config,builder.createTopology());
            Thread.sleep(3000000);
            cluster.shutdown();
        }
    }

}
