package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SourceType;
import com._4paradigm.csp.operator.factory.DataSinkFactory;
import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.factory.convert.MergeConvert;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.csp.operator.utils.SqlUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.UUID;

import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_SINK_PARALLELISM;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_SINK_PARALLELISM_DEFAULT;


/**
 * 文本引入算子 csv/tsv/txt/parquet/orc -> kafka 流式引入
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/filesystem_sink.html
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/
 * <p>
 * parquet: https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/streamfile_sink.html
 */
@Slf4j
public class StreamingJob {
    /**
     * @param args see application.properties {@link com._4paradigm.csp.operator.constant.PropertiesConstants}
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        log.info("Stream Job args: {}", JSON.toJSONString(args));

        // prepare
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        CheckPointUtil.setCheckpointConfig(env, parameterTool);

        // run
        run(env, parameterTool);
    }

    public static void run(final StreamExecutionEnvironment env, final ParameterTool parameterTool) throws Exception {
        // 1. 读数据源
        DataStream dataStreamSource = DataSourceFactory.readDataSource(env, parameterTool);
//        dataStreamSource.print();

        // 2. 对数据源进行 sql 处理
//        DataStream sqlStream = executeSQL(env, parameterTool, dataStreamSource);

        // 2.1 merge feats
//        DataStream mergeStream = executeMerge(parameterTool, sqlStream);

        // 3. 输出到 Kafka
        SinkFunction kafkaSink = DataSinkFactory.sink(parameterTool);
//        mergeStream.addSink(kafkaSink)
//                .setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, STREAM_SINK_PARALLELISM_DEFAULT))
//                .name("csp.stream.sink")
//                .uid("sink-kafka-id");
//        mergeStream.print();

        env.execute(parameterTool.get(PropertiesConstants.JOB_NAME, "CSP StreamJob"));
    }

    /**
     * 输出时 merge 非 id 的所有字段
     */
//    private static DataStream executeMerge(ParameterTool parameterTool, DataStream sqlStream) {
//        if (!parameterTool.getBoolean(PropertiesConstants.SINK_MERGE_ENABLE, false)) {
//            return sqlStream;
//        }
//        // 除了 id, 字段全 merge 到 field 中
//        MergeConvert convert = new MergeConvert(parameterTool);
//        return sqlStream.flatMap(convert).uid("converter-merge-fields-op-id");
//    }
//
//    /**
//     * Flink sql 处理
//     */
//    private static DataStream executeSQL(final StreamExecutionEnvironment env,
//                                         final ParameterTool parameterTool,
//                                         final DataStream dataStreamSource) {
//        Boolean enableSql = parameterTool.getBoolean(PropertiesConstants.STREAM_SQL_ENABLE, false);
//        String sourceTypeStr = parameterTool.getRequired(PropertiesConstants.SOURCE_TYPE);
//        SourceType sourceType = SourceType.valueOf(sourceTypeStr.toLowerCase());
//
//        if (!SourceType.kafka.equals(sourceType) && !enableSql) {
//            return dataStreamSource;
//        }
//        return SqlUtil.executeSql(dataStreamSource, env, parameterTool);
//    }
}
