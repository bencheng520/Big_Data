package com.ben.storm.count.spout;


import org.apache.storm.shade.org.apache.commons.io.FileUtils;
import org.apache.storm.shade.org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @Author 001289
 * @Date 2018/3/26 21:05
 * @Description ${DESCRIPTION}
 */
public class FileSentenceSpout extends BaseRichSpout {

    private static final Logger logger = LoggerFactory.getLogger(FileSentenceSpout.class);
    private String inputPath;
    SpoutOutputCollector collector;
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector = spoutOutputCollector;
        inputPath = (String) map.get("INPUT_PATH");
    }

    public void nextTuple() {
        Utils.sleep(1000);
        //获取数据
        Collection<File> files = FileUtils.listFiles(new File(inputPath),
                FileFilterUtils.notFileFilter(FileFilterUtils.suffixFileFilter(".bak")), null);
        for (File f : files) {
            try {
                List<String> lines = FileUtils.readLines(f, "UTF-8");
                for (String sentence : lines) {
                    logger.info("spout: {}", sentence);
                    collector.emit(new Values(sentence));
                }
                FileUtils.moveFile(f, new File(f.getPath() + System.currentTimeMillis() + ".bak"));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("sentence"));
    }
}
