package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import com._4paradigm.rtidb.client.TableSyncClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.RTIDBClientConfig;
import com._4paradigm.rtidb.client.ha.impl.NameServerClientImpl;
import com._4paradigm.rtidb.client.ha.impl.RTIDBClusterClient;
import com._4paradigm.rtidb.client.impl.TableSyncClientImpl;
import java.io.IOException;
import java.util.concurrent.TimeoutException;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

@Slf4j
public class RtidbSinkJob {

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

        DataStream redisSource = dataStreamSource.map((MapFunction<Row, Tuple2<String, String>>) row -> {
            String[] test = row.toString().split(",");
            return new Tuple2(test[0], test[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {});


        RtidbOutputFormat rtidbOutputFormat = new RtidbOutputFormat();
        rtidbOutputFormat.setZkAddress(parameterTool.get("zkAddress"));
        rtidbOutputFormat.setZkPath(parameterTool.get("zkPath"));
        rtidbOutputFormat.setTableName(parameterTool.get("table"));

        redisSource.writeUsingOutputFormat(rtidbOutputFormat).name("Rtidb Sink");

        env.execute("Rtidb sink Example");
    }

    private static class RtidbOutputFormat implements OutputFormat<Tuple2<String, String>> {

        private static RTIDBClusterClient clusterClient = null;
        // 发送同步请求的client
        private static TableSyncClient tableSyncClient = null;

        private String zkAddress = null;
        private String zkPath = null;
        private String tableName = null;

        public void setZkAddress(String zkAddress) {
            this.zkAddress = zkAddress;
        }

        public void setZkPath(String zkPath) {
            this.zkPath = zkPath;
        }

        public void setTableName(String tableName) {
            this.tableName = tableName;
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
        public void writeRecord(Tuple2<String, String> record) throws IOException {

            long ts = System.currentTimeMillis();
            // 通过返回值可以判断是否插入成功
            boolean ret = false;
            try {
                ret = tableSyncClient.put(tableName, record.f0, ts, record.f1);
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
