package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.ConnectorType;
import com._4paradigm.csp.operator.factory.source.KafkaSourceReader;
import com._4paradigm.csp.operator.model.ConnectorItem;
import com._4paradigm.csp.operator.model.StreamConfigItem;
import com._4paradigm.csp.operator.model.StreamConfigItem.KafkaConfig;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.DataConvertUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.csp.operator.utils.SchemaUtil;
import com._4paradigm.pdms.telamon.v1.rest.request.JDBCRequest.JDBCSource;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class JDBCSinkExample2 {

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

        StreamConfigItem sinkStream = sinkConnectorList.get(0).getStreamConfigItems().stream().filter(e->e.getConnectorType().equals(ConnectorType.JDBC)).findFirst().get();
        JDBCSource jdbcSource = sinkStream.getConnectorConfig().getJdbcConfig();

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
        String table = sinkStream.getConnectorConfig().getParameters().get("table");
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

        dataStreamSource.writeUsingOutputFormat(jdbcOutput).name("JDBC Sink");

        // execute program
        env.execute("JDBC sink Example");
    }


}
