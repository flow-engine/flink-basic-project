package com._4paradigm.csp.operator.utils;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.CheckPointType;
import com._4paradigm.csp.operator.enums.SourceType;
import com.google.common.collect.Maps;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_ENABLE;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_TYPE;

@Slf4j
public class ExecutionEnvUtil {
    public static ParameterTool createParameterTool(final String[] args) throws Exception {
        ParameterTool defaultConfig = ParameterTool
                .fromPropertiesFile(ExecutionEnvUtil.class.getResourceAsStream(PropertiesConstants.PROPERTIES_FILE_NAME));
        ParameterTool userConfig = ParameterTool.fromArgs(args);
        ParameterTool systemProperties = ParameterTool.fromSystemProperties();
        log.info("Default config: {}", defaultConfig.getProperties());
        log.info("User config: {}", userConfig.getProperties());
        log.info("System Properties: {}", systemProperties.getProperties());

        ParameterTool parameterTool = defaultConfig
                .mergeWith(userConfig)
                .mergeWith(systemProperties);
        if (SourceType.kafka_deduplication.name()
                .equals(parameterTool.get(PropertiesConstants.SOURCE_TYPE).toLowerCase())) {
            log.info("Kafka Deduplication Job force checkpoint RocksDB");
            // 去重算子强制 RocksDB
            Map<String, String> deDuplicationForceConfig = Maps.newHashMap();
            deDuplicationForceConfig.put(STREAM_CHECKPOINT_ENABLE, "true");
            deDuplicationForceConfig.put(STREAM_CHECKPOINT_TYPE, CheckPointType.rocksdb.name());
            parameterTool.mergeWith(ParameterTool.fromMap(deDuplicationForceConfig));
        }
        return parameterTool;
    }

    public static StreamExecutionEnvironment prepare(ParameterTool parameterTool) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parameterTool.getInt(PropertiesConstants.STREAM_PARALLELISM, 1));
//        env.getConfig().disableSysoutLogging();
        // https://ci.apache.org/projects/flink/flink-docs-stable/dev/task_failure_recovery.html
        env.getConfig().setRestartStrategy(
                RestartStrategies.fixedDelayRestart(parameterTool.getInt(PropertiesConstants.JOB_RETRY_MAX_NUMBER_RESTART_ATTEMPTS,4),
                        parameterTool.getLong(PropertiesConstants.JOB_RETRY_BACK_OFF_TIMEMS, 60000)));
        env.getConfig().setGlobalJobParameters(parameterTool);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        return env;
    }
}