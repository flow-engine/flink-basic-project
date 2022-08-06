package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.factory.RedisSinkFactory.RedisExampleMapper;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.types.Row;

@Slf4j
public class ConnectorJob {

    /**
     * @param args see application.properties {@link PropertiesConstants}
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        log.info("Job args: {}", args);

        // prepare
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        StreamExecutionEnvironment env = ExecutionEnvUtil.prepare(parameterTool);
        CheckPointUtil.setCheckpointConfig(env, parameterTool);
        DataStream dataStreamSource = DataSourceFactory.readDataSource(env, parameterTool);

        log.info("Job sourceConnector: {}", parameterTool.get(PropertiesConstants.SOURCE_CONNECTOR));
        log.info("Job sinkConnector: {}", parameterTool.get(PropertiesConstants.SINK_CONNECTOR));

        DataStream redisSource = dataStreamSource.map((MapFunction<Row, Tuple2<String, String>>) row -> {
            String[] test = row.toString().split(",");
            return new Tuple2(test[0], test[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {});

        String redisHost = parameterTool.get(PropertiesConstants.SINK_REDIS_HOST);

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost(redisHost).build();
        redisSource.addSink(new RedisSink<>(conf, new RedisExampleMapper()));

//        dataStreamSource.addSink(new FlinkDemoRedisSink());
        env.execute(parameterTool.get(PropertiesConstants.JOB_NAME, "redis Job"));
    }
}
