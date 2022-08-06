package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SyncEsType;
import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.factory.ElasticSearchSinkFactory;
import com._4paradigm.csp.operator.factory.convert.EsItemExistConvert;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.csp.operator.utils.SqlUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;

import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_SINK_PARALLELISM;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_SINK_PARALLELISM_DEFAULT;

/**
 * https://github.com/apache/flink/blob/master/flink-end-to-end-tests/flink-elasticsearch6-test/src/main/java/org/apache/flink/streaming/tests/Elasticsearch6SinkExample.java
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/elasticsearch.html
 */
@Slf4j
public class Elasticsearch6SinkJob {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        CheckPointUtil.setCheckpointConfig(env, parameterTool);

//        run(env, parameterTool);
    }

//    public static void run(final StreamExecutionEnvironment env, final ParameterTool parameterTool) throws Exception {
//        DataStream dataStreamSource = DataSourceFactory.readDataSource(env, parameterTool);
//
//        if (parameterTool.getBoolean(PropertiesConstants.STREAM_SQL_ENABLE, false)) {
//            DataStream<String> sqlStream = SqlUtil.executeSql(dataStreamSource, env, parameterTool);
//            sink(sqlStream, parameterTool);
//        } else {
//            sink(dataStreamSource, parameterTool);
//        }
//
//        env.execute(parameterTool.get(PropertiesConstants.JOB_NAME, "CSP ES Job"));
//    }
//
//    private static void sink(DataStream dataStreamSource, ParameterTool parameterTool) throws Exception {
//        ElasticsearchSink sink = ElasticSearchSinkFactory.sink(parameterTool);
//        SyncEsType syncEsType = SyncEsType.valueOf(parameterTool.get(PropertiesConstants.SINK_ES_SYNC_TYPE).toLowerCase());
//        if (SyncEsType.full.equals(syncEsType)) {
//            dataStreamSource.addSink(sink)
//                    .setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, STREAM_SINK_PARALLELISM_DEFAULT))
//                    .name("csp.es6.sink.insert")
//                    .uid("sink-es6-full-insert-id");
//        } else {
//            // upsert
//            String hosts = parameterTool.get(PropertiesConstants.SINK_ES_HOST);
//            String index = parameterTool.get(PropertiesConstants.SINK_ES_INDEX);
//            String type = parameterTool.get(PropertiesConstants.SINK_ES_TYPE);
//            String uniqueColumn = parameterTool.get(PropertiesConstants.SINK_ES_UNIQUE_COLUMN);
//            EsItemExistConvert esItemExistConvert = new EsItemExistConvert(hosts, index, type, uniqueColumn);
//            // 查询之前的数据是否存在
//            SingleOutputStreamOperator existStream = dataStreamSource.flatMap(esItemExistConvert)
//                    .uid("converter-es-id-exist-id");
//            // upsert 转换 Stream
//            existStream.addSink(sink)
//                    .setParallelism(parameterTool.getInt(STREAM_SINK_PARALLELISM, STREAM_SINK_PARALLELISM_DEFAULT))
//                    .name("csp.es6.sink.upsert")
//                    .uid("sink-es6-upsert-id");
//        }
//    }
}
