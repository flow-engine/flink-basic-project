package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SinkType;
import com._4paradigm.csp.operator.enums.SourceType;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.csp.operator.utils.GitUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class Main {
    public static void main(String[] args) throws Exception {
        GitUtil.printGitInfo();
        log.info("Job args: {}", JSON.toJSONString(args));

        // prepare
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        log.info("All args: {}", parameterTool.getProperties());

        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        CheckPointUtil.setCheckpointConfig(env, parameterTool);

        String sourceTypeStr = parameterTool.getRequired(PropertiesConstants.SOURCE_TYPE);
        SourceType sourceType = SourceType.valueOf(sourceTypeStr.toLowerCase());

        String sinkTypeStr = parameterTool.get(PropertiesConstants.SINK_TYPE, SinkType.kafka.name());
        SinkType sinkType = SinkType.valueOf(sinkTypeStr.toLowerCase());
        log.info("SourceType {} -> SinkType {}", sourceType, sinkType);

        if (SourceType.jdbc.equals(sourceType)) {
            // jdbc -> kafka
            BatchJob.run(env, parameterTool);
        } else if (SourceType.kafka_deduplication.equals(sourceType)) {
            KeyedStateDeduplicationJob.run(env, parameterTool);
//        } else if (SinkType.es.equals(sinkType)) {
//            Elasticsearch6SinkJob.run(env, parameterTool);
//        } else {
            // fs(csv/tsv/txt/orc/parquet)/kafka_sql -> kafka
            StreamingJob.run(env, parameterTool);
        }
    }
}
