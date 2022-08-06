package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.KafkaFactory;
import com._4paradigm.csp.operator.filter.KeyedStateFilter;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.csp.operator.utils.KeyUtils;
import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;

import java.util.Arrays;
import java.util.LinkedHashSet;

/**
 * Kafka -> Kafka 去重算子
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html
 */
@Slf4j
public class KeyedStateDeduplicationJob {

    /**
     * 参数 -- keys
     *
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // prepare
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        CheckPointUtil.setCheckpointConfig(env, parameterTool);

        run(env, parameterTool);
    }

    public static void run(final StreamExecutionEnvironment env, final ParameterTool parameterTool) throws Exception {
        FlinkKafkaConsumerBase<String> kafkaConsumer = KafkaFactory.consumer(parameterTool, new HashMap<>());
        SinkFunction<String> kafkaProducer = KafkaFactory.sink(parameterTool, new HashMap<>());

        String keyStr = parameterTool.getRequired(PropertiesConstants.UNIQUE_KEYS);
        LinkedHashSet<String> keys = new LinkedHashSet<>(Arrays.asList(keyStr.split(",")));
        log.info("Unique keyStr [{}]", keyStr);
        log.info("Unique key sets [{}]", keys);

        env.addSource(kafkaConsumer)
                .uid("source-kafka-id")
                .keyBy(json -> KeyUtils.md5KeysContent(json, keys))
                .filter(new KeyedStateFilter())
                .uid("filter-unique-keys-id")
                .addSink(kafkaProducer)
                .uid("sink-kafka-id")
                .name("csp.deDuplication.sink");

        env.execute(parameterTool.get(PropertiesConstants.JOB_NAME, "CSP Stream KeyedStateDeduplication Job"));
    }
}
