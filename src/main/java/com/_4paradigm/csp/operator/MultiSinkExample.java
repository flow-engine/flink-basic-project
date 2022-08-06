package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.ConnectorType;
import com._4paradigm.csp.operator.factory.ElasticSearchSinkFactory;
import com._4paradigm.csp.operator.factory.KafkaFactory;
import com._4paradigm.csp.operator.factory.convert.RowWithSchemaConvert;
import com._4paradigm.csp.operator.factory.source.KafkaSourceReader;
import com._4paradigm.csp.operator.model.ConnectorItem;
import com._4paradigm.csp.operator.model.StreamConfigItem;
import com._4paradigm.csp.operator.model.StreamConfigItem.EsConfig;
import com._4paradigm.csp.operator.model.StreamConfigItem.KafkaConfig;
import com._4paradigm.csp.operator.model.StreamConfigItem.RtidbConfig;
import com._4paradigm.csp.operator.sink.Elasticsearch6Sink2Function;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.DataConvertUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.csp.operator.utils.SchemaUtil;
import com._4paradigm.pdms.telamon.model.CommonSchemaTerm;
import com._4paradigm.pdms.telamon.v1.rest.request.JDBCRequest.JDBCSource;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import com.fasterxml.jackson.core.type.TypeReference;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

@Slf4j
public class MultiSinkExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        CheckPointUtil.setCheckpointConfig(env, parameterTool);
//        DataStream dataStreamSource = DataSourceFactory.readDataSource(env, parameterTool);

        log.info("Job sourceConnector: {}", parameterTool.get(PropertiesConstants.SOURCE_CONNECTOR));
        log.info("Job sinkConnector: {}", parameterTool.get(PropertiesConstants.SINK_CONNECTOR));

        String sourceConnectorJson = parameterTool.get(PropertiesConstants.SOURCE_CONNECTOR);
        String sinkConnectorJson = parameterTool.get(PropertiesConstants.SINK_CONNECTOR);

        List<ConnectorItem> sourceConnectorList = DataConvertUtil.jsonMapper.readValue(sourceConnectorJson, new TypeReference<List<ConnectorItem>>(){});
        List<ConnectorItem> sinkConnectorList = DataConvertUtil.jsonMapper.readValue(sinkConnectorJson,new TypeReference<List<ConnectorItem>>(){});

        StreamConfigItem sourceStream = sourceConnectorList.get(0).getStreamConfigItems().stream().filter(e->e.getConnectorType().equals(ConnectorType.KAFKA)).findFirst().get();
        KafkaConfig sourceKafka = sourceStream.getConnectorConfig().getKafkaConfig();
        Map<String, String> sourceParameter = new HashMap<>();
        sourceParameter.put(PropertiesConstants.SOURCE_KAFKA_BROKERS, sourceKafka.getKafkaEndpoint());
        sourceParameter.put(PropertiesConstants.SOURCE_KAFKA_TOPIC, sourceKafka.getTopic());
        ParameterTool sourceParameterTool = ParameterTool.fromMap(sourceParameter);

        parameterTool.mergeWith(sourceParameterTool);

        KafkaSourceReader kafkaSourceReader = new KafkaSourceReader();
        DataStream<Row> dataStreamSource = kafkaSourceReader.readDataSourceStreamWithSchema(env, parameterTool,
                sourceConnectorList.get(0).getGroupSchema(), sourceParameter);

        for (StreamConfigItem streamConfigItem: sinkConnectorList.get(0).getStreamConfigItems()) {

            if (streamConfigItem.getConnectorType().equals(ConnectorType.JDBC)) {
                JDBCSource jdbcSource = streamConfigItem.getConnectorConfig().getJdbcConfig();

                int[] schemaArray = sinkConnectorList.get(0).getGroupSchema().stream()
                        .map(e -> SchemaUtil.forSqlFromPdms(e.getType()))
                        .mapToInt(i -> i).toArray();

                StringBuilder sql2 = new StringBuilder("(?");
                for (int i = 0; i < schemaArray.length - 1; i++) {
                    sql2.append(",?");
                }
                sql2.append(")");

                String url = String.format("jdbc:mysql://%s:%d/%s", jdbcSource.getHost(), jdbcSource.getPort(),
                        jdbcSource.getDbname())
                        + "?useUnicode=yes&characterEncoding=UTF-8&createDatabaseIfNotExist=true&serverTimezone=GMT%2B8";
                String user = jdbcSource.getUsername();
                String password = jdbcSource.getPassword();
                String table = streamConfigItem.getConnectorConfig().getParameters().get("table");
//        String table = jdbcSource.getTable();

                JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                        .setDrivername("com.mysql.cj.jdbc.Driver")
                        .setDBUrl(url)
                        .setUsername(user)
                        .setPassword(password)
                        .setSqlTypes(schemaArray)
                        .setQuery(String.format("INSERT INTO %s VALUES %s", table, sql2.toString()))
                        .setBatchInterval(1)
                        .finish();

                dataStreamSource.writeUsingOutputFormat(jdbcOutput).name("JDBC sink");
            }

            if (streamConfigItem.getConnectorType().equals(ConnectorType.RTIDB)) {

                RtidbConfig rtidbConfig = streamConfigItem.getConnectorConfig().getRtidbConfig();

                RtidbOutputFormat rtidbOutputFormat = new RtidbOutputFormat();
                rtidbOutputFormat.setZkAddress(rtidbConfig.getZkEndpoints());
                rtidbOutputFormat.setZkPath(rtidbConfig.getZkRootPath());
                rtidbOutputFormat.setTableName(rtidbConfig.getTableName());
                String[] fieldArray = sinkConnectorList.get(0).getGroupSchema().stream()
                        .map(CommonSchemaTerm::getName).toArray(String[]::new);
                rtidbOutputFormat.setField(fieldArray);

                dataStreamSource.writeUsingOutputFormat(rtidbOutputFormat).name("Rtidb Sink");
            }

            if (streamConfigItem.getConnectorType().equals(ConnectorType.KAFKA)) {
                KafkaConfig sinkKafka = streamConfigItem.getConnectorConfig().getKafkaConfig();
                Map<String, String> sinkParameter = new HashMap<>();
                sinkParameter.put(PropertiesConstants.SINK_KAFKA_BROKERS, sinkKafka.getKafkaEndpoint());
                sinkParameter.put(PropertiesConstants.SINK_KAFKA_TOPIC, sinkKafka.getTopic());
                SinkFunction kafkaSink = KafkaFactory.sink(parameterTool, sinkParameter);

                RowWithSchemaConvert rowConvert = new RowWithSchemaConvert(
                        sinkConnectorList.get(0).getGroupSchema().stream()
                                .map(CommonSchemaTerm::getName).toArray(String[]::new));
                dataStreamSource.flatMap(rowConvert).addSink(kafkaSink).name("Kafka sink");
            }

            if (streamConfigItem.getConnectorType().equals(ConnectorType.ELASTICSEARCH)) {
                EsConfig esConfig = streamConfigItem.getConnectorConfig().getEsConfig();

                List<HttpHost> httpHosts = new ArrayList<>();
                httpHosts.add(HttpHost.create(esConfig.getUrl()));

                ElasticsearchSink.Builder<Row> esSinkBuilder = new ElasticsearchSink.Builder<>(
                        httpHosts,
                        new Elasticsearch6Sink2Function(esConfig.getIndex(), esConfig.getType()));

                // this instructs the sink to emit after every element, otherwise they would be buffered
                esSinkBuilder.setBulkFlushMaxActions(1);

                dataStreamSource.addSink(esSinkBuilder.build());
            }

        }

        // execute program
        env.execute("Multi sink Example");
    }

    private static class RtidbOutputFormat implements OutputFormat<Row> {

        private static RTIDBClusterClient clusterClient = null;
        // 发送同步请求的client
        private static TableSyncClient tableSyncClient = null;

        private String zkAddress = null;
        private String zkPath = null;
        private String tableName = null;
        private String[] field = null;

        public void setZkAddress(String zkAddress) {
            this.zkAddress = zkAddress;
        }

        public void setZkPath(String zkPath) {
            this.zkPath = zkPath;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
        }

        public void setField(String[] field) {
            this.field = field;
        }

        @Override
        public void configure(Configuration parameters) {
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            try {
                RTIDBClientConfig config = new RTIDBClientConfig();

                config.setZkEndpoints(zkAddress);
                config.setZkRootPath(zkPath);

                clusterClient = new RTIDBClusterClient(config);
                clusterClient.init();

                tableSyncClient = new TableSyncClientImpl(clusterClient);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        @Override
        public void writeRecord(Row record) throws IOException {

            long ts = System.currentTimeMillis();
            // 通过返回值可以判断是否插入成功
            boolean ret = false;
            try {
                ret = tableSyncClient.put(tableName, record.getField(0).toString(), ts, record.getField(1).toString());
            } catch (TimeoutException | TabletException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void close() throws IOException {
//            nsc.close();
        }

    }

}
