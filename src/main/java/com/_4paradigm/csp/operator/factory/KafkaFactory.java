package com._4paradigm.csp.operator.factory;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer09;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer09;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Optional;
import java.util.Properties;

import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_MODE;

/**
 * 官网 Kafka 版本对比
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/kafka.html
 */
@Slf4j
public class KafkaFactory {

    private static final String DEFAULT_VERSION = "0.11";

    private static final String SINK_KAFKA_PREFIX = "sink.kafka.";
    private static final String SOURCE_KAFKA_PREFIX = "source.kafka.";

    public static SinkFunction sink(final ParameterTool parameterTool, Map<String, String> sourceParameter) {
        String version = parameterTool.get(PropertiesConstants.SINK_KAFKA_VERSION, DEFAULT_VERSION);
        String brokers = parameterTool.get(PropertiesConstants.SINK_KAFKA_BROKERS);
        String topic = parameterTool.get(PropertiesConstants.SINK_KAFKA_TOPIC);
        if (!sourceParameter.isEmpty()) {
            brokers = sourceParameter.get(PropertiesConstants.SINK_KAFKA_BROKERS);
            topic = sourceParameter.get(PropertiesConstants.SINK_KAFKA_TOPIC);
        }
        log.info("Kafka Sink properties: version [{}], brokers [{}], topic [{}]", version, brokers, topic);

        SerializationSchema schema = new SimpleStringSchema();
        KeyedSerializationSchemaWrapper serializationSchema = new KeyedSerializationSchemaWrapper(schema);

        Properties producerProperties = new Properties();
        parameterTool.getProperties().forEach((key, value) -> {
            if (key.toString().startsWith(SINK_KAFKA_PREFIX)) {
                producerProperties.put(key.toString().substring(SINK_KAFKA_PREFIX.length()), value);
            }
        });
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        CheckpointingMode checkpointingMode = CheckpointingMode.valueOf(parameterTool.get(STREAM_CHECKPOINT_MODE, CheckpointingMode.AT_LEAST_ONCE.name()).toUpperCase());
        if (CheckpointingMode.EXACTLY_ONCE.equals(checkpointingMode)) {
            // https://ci.apache.org/projects/flink/flink-docs-release-1.10/zh/dev/connectors/kafka.html
            // https://www.cnblogs.com/wangzhuxing/p/10111831.html
            // https://www.dazhuanlan.com/2019/08/25/5d61ab4edaf4c/
            // https://www.cnblogs.com/createweb/p/11971846.html
            // https://kafka.apache.org/0110/documentation.html#producerconfigs
            // Kafka 服务端默认等待事务状态变更的最长时间为 15min, 客户端事务不能超过这个值
            producerProperties.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,
                    parameterTool.get(SINK_KAFKA_PREFIX + ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "890000")); // max: 900000
            /**
             * 系统自动设置:
             * 开启幂等性 enable.idempotence=true
             * Partition 发生 leader 切换时, Flink 不重启, 自动尝试 retries=2147483647
             * 阻塞前客户端在单个连接上发送的未确认请求的最大数量 max.in.flight.requests.per.connection=1
             * exactly-once 必须等所有 broker 确认 acks=all
             *
             * 注意: 启用幂等需要使用max.in.flight.requests.per.connection,连接小于或等于5，重试大于0且ack必须为all
             */
        }
        producerProperties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
                parameterTool.get(SINK_KAFKA_PREFIX + ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "180000")); // 默认 30s 增大为 3min
        producerProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG,
                parameterTool.get(SINK_KAFKA_PREFIX + ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")); // 默认 producer
        log.info("Kafka Sink properties: {}", producerProperties);

        switch (version) {
            case "0.9":
                return new FlinkKafkaProducer09<>(
                        topic,
                        serializationSchema,
                        producerProperties,
                        (FlinkKafkaPartitioner) null);
            case "0.10":
                return new FlinkKafkaProducer010<>(
                        topic,
                        serializationSchema,
                        producerProperties,
                        (FlinkKafkaPartitioner) null);
            default:
                // default 0.11
                return CheckpointingMode.EXACTLY_ONCE.equals(checkpointingMode) ?
                        new FlinkKafkaProducer011<>(topic,
                                serializationSchema,
                                producerProperties,
                                Optional.empty(),
                                FlinkKafkaProducer011.Semantic.EXACTLY_ONCE, 5)
                        :
                        new FlinkKafkaProducer011<>(topic,
                                serializationSchema,
                                producerProperties, Optional.empty());
        }
    }

    public static FlinkKafkaConsumerBase<String> consumer(ParameterTool parameterTool, Map<String, String> sourceParameter) {
        String version = parameterTool.get(PropertiesConstants.SOURCE_KAFKA_VERSION, DEFAULT_VERSION);
        String brokers = parameterTool.get(PropertiesConstants.SOURCE_KAFKA_BROKERS);
        String topic = parameterTool.get(PropertiesConstants.SOURCE_KAFKA_TOPIC);
        if (!sourceParameter.isEmpty()) {
            brokers = sourceParameter.get(PropertiesConstants.SOURCE_KAFKA_BROKERS);
            topic = sourceParameter.get(PropertiesConstants.SOURCE_KAFKA_TOPIC);
        }
        String reset = parameterTool.get(PropertiesConstants.CONSUMER_KAFKA_AUTO_OFFSET_RESET, "latest");
        String groupId = parameterTool.get(PropertiesConstants.CONSUMER_GROUP_ID, String.valueOf(System.currentTimeMillis()));
        log.info("Kafka Source properties: version [{}], brokers [{}], topic [{}], reset [{}], groupId [{}]",
                version, brokers, topic, reset, groupId);

        Properties consumerConfigs = new Properties();
        // prepare consumerConfig
        parameterTool.getProperties().forEach((key, value) -> {
            if (key.toString().startsWith(SOURCE_KAFKA_PREFIX)) {
                consumerConfigs.put(key.toString().substring(SOURCE_KAFKA_PREFIX.length()), value);
            }
        });
        consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        consumerConfigs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, reset);
        consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        consumerConfigs.setProperty("isolation.level", "read_committed");
        log.info("Kafka Source properties: {}", consumerConfigs);

        switch (version) {
            case "0.9":
                return new FlinkKafkaConsumer09<>(
                        topic,
                        new SimpleStringSchema(),
                        consumerConfigs
                );
            case "0.10":
                return new FlinkKafkaConsumer010<>(
                        topic,
                        new SimpleStringSchema(),
                        consumerConfigs
                );
            default:
                // default 0.10
                return new FlinkKafkaConsumer011<>(
                        topic,
                        new SimpleStringSchema(),
                        consumerConfigs
                );
        }
    }
}
