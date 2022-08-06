package com._4paradigm.csp.operator.model;

import com._4paradigm.csp.operator.enums.ConnectorType;
import com._4paradigm.pdms.telamon.v1.rest.request.JDBCRequest;
import java.util.HashMap;
import java.util.Map;
import lombok.Data;

@Data
public class StreamConfigItem {
    private ConnectorType connectorType;
    private Config connectorConfig;

    @Data
    public static class Config {
        private KafkaConfig kafkaConfig = new KafkaConfig();
        private JDBCRequest.JDBCSource jdbcConfig = new JDBCRequest.JDBCSource();
        private RtidbConfig rtidbConfig = new RtidbConfig();
        private EsConfig esConfig = new EsConfig();
        private Map<String, String> parameters =  new HashMap<>();
    }

    @Data
    public static class KafkaConfig {
        private String kafkaEndpoint;
        private String zkEndPoint;
        private String topic;
        private String groupId;
        private String clientId;
    }

    @Data
    public static class RtidbConfig {
        private String zkEndpoints;
        private String zkRootPath;
        private String tableName;
    }

    @Data
    public static class EsConfig {
        private String url;
        private String index;
        private String type;
    }
}
