package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.DataSinkFactory;
import com._4paradigm.csp.operator.source.JDBCSource;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_SINK_PARALLELISM;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_SINK_PARALLELISM_DEFAULT;

/**
 * JDBC(目前只提供了 Hive Driver) -> Kafka 定时引入
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/table/hive/
 * <p>
 * read: https://blog.51cto.com/12597095/2439413
 * output: https://blog.csdn.net/xsdxs/article/details/82533616
 */
@Slf4j
public class BatchJob {

    public static void main(String[] args) throws Exception {
        log.info("Batch Job args: {}", JSON.toJSONString(args));
        // prepare
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        CheckPointUtil.setCheckpointConfig(env, parameterTool);
        // run
        run(env, parameterTool);
    }

    public static void run(final StreamExecutionEnvironment env, final ParameterTool parameterTool) throws Exception {
        String driver = parameterTool.get(PropertiesConstants.SOURCE_DB_DRIVER, PropertiesConstants.SOURCE_DB_DRIVER_DEFAULT);
        String url = parameterTool.getRequired(PropertiesConstants.SOURCE_DB_URL);
        String user = parameterTool.getRequired(PropertiesConstants.SOURCE_DB_USERNAME);
        String password = parameterTool.getRequired(PropertiesConstants.SOURCE_DB_PASSWORD);
        String sql = parameterTool.getRequired(PropertiesConstants.SOURCE_DB_SQL);
        int fetchSize = parameterTool.getInt(PropertiesConstants.SOURCE_DB_FETCH_SIZE, PropertiesConstants.SOURCE_DB_FETCH_SIZE_DEFAULT);

        JDBCSource jdbcSource = new JDBCSource(driver, url, user, password, sql, fetchSize);
        SinkFunction kafkaSink = DataSinkFactory.sink(parameterTool);
        env.addSource(jdbcSource)
                .uid("source-jdbc-id")
                .addSink(kafkaSink)
                .uid("sink-kafka-id")
                .setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, STREAM_SINK_PARALLELISM_DEFAULT))
                .name("csp.batch.sink");

        env.execute(parameterTool.get(PropertiesConstants.JOB_NAME, "CSP BatchJob"));
    }
}
