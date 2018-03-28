package com.ben.storm.count.builder;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.storm.kafka.spout.KafkaSpoutTupleBuilder;
import org.apache.storm.tuple.Values;

import java.util.List;

/**
 * @Author 001289
 * @Date 2018/3/28 10:56
 * @Description ${DESCRIPTION}
 */
public class KafkaTopicTupleBuilder<K, V> extends KafkaSpoutTupleBuilder<K, V> {

    private static final long serialVersionUID = -8676754840657764583L;

    public KafkaTopicTupleBuilder(String... topics) {
        super(topics);
    }

    public List<Object> buildTuple(ConsumerRecord<K, V> consumerRecord) {
        return new Values(new Object[]{consumerRecord.value()});
    }

}
