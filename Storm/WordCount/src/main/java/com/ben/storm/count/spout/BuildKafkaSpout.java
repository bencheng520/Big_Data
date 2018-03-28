package com.ben.storm.count.spout;

import com.ben.storm.count.builder.KafkaTopicTupleBuilder;
import org.apache.storm.kafka.spout.*;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author 001289
 * @Date 2018/3/28 10:39
 * @Description ${DESCRIPTION}
 */
public class BuildKafkaSpout {
    private String[] topics;
    private String brokerUrl;
    private String groupId;
    public BuildKafkaSpout(String brokerUrl, String groupId, String... topics) {
        this.topics = topics;
        this.brokerUrl = brokerUrl;
        this.groupId = groupId;
    }
    public KafkaSpout<String, String> build() {
        Map<String, Object> kafkaConsumerProps = new HashMap();
        kafkaConsumerProps.put("bootstrap.servers", this.brokerUrl);
        kafkaConsumerProps.put("group.id", this.groupId);
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500L), KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2L), 2147483647, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10L));
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = (new KafkaSpoutConfig.Builder(kafkaConsumerProps, this.getKafkaSpoutStreams(), this.getTuplesBuilder(), retryService)).setOffsetCommitPeriodMs(10000L).setFirstPollOffsetStrategy(KafkaSpoutConfig.FirstPollOffsetStrategy.UNCOMMITTED_EARLIEST).setMaxUncommittedOffsets(100000).build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        return kafkaSpout;
    }

    public KafkaSpout<String, String> build(KafkaSpoutConfig.FirstPollOffsetStrategy strategy) {
        Map<String, Object> kafkaConsumerProps = new HashMap();
        kafkaConsumerProps.put("bootstrap.servers", this.brokerUrl);
        kafkaConsumerProps.put("group.id", this.groupId);
        kafkaConsumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        kafkaConsumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaSpoutRetryService retryService = new KafkaSpoutRetryExponentialBackoff(KafkaSpoutRetryExponentialBackoff.TimeInterval.microSeconds(500L), KafkaSpoutRetryExponentialBackoff.TimeInterval.milliSeconds(2L), 2147483647, KafkaSpoutRetryExponentialBackoff.TimeInterval.seconds(10L));
        KafkaSpoutConfig<String, String> kafkaSpoutConfig = (new KafkaSpoutConfig.Builder(kafkaConsumerProps, this.getKafkaSpoutStreams(), this.getTuplesBuilder(), retryService)).setOffsetCommitPeriodMs(10000L).setFirstPollOffsetStrategy(strategy).setMaxUncommittedOffsets(100000).build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout(kafkaSpoutConfig);
        return kafkaSpout;
    }

    protected KafkaSpoutTuplesBuilder<String, String> getTuplesBuilder() {
        return (new org.apache.storm.kafka.spout.KafkaSpoutTuplesBuilderNamedTopics.Builder(new KafkaSpoutTupleBuilder[]{new KafkaTopicTupleBuilder(this.topics)})).build();
    }

    protected KafkaSpoutStreams getKafkaSpoutStreams() {
        Fields outputFields = new Fields(new String[]{"str"});
        return (new org.apache.storm.kafka.spout.KafkaSpoutStreamsNamedTopics.Builder(outputFields, this.topics)).build();
    }
}
