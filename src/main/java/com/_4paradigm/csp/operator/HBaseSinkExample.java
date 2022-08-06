package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import java.io.IOException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseSinkExample {

    public static void main(String[] args) throws Exception {
        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        CheckPointUtil.setCheckpointConfig(env, parameterTool);
        DataStream dataStreamSource = DataSourceFactory.readDataSource(env, parameterTool);

        DataStream dataStream = dataStreamSource.map((MapFunction<Row, Tuple2<String, String>>) row -> {
            String[] test = row.toString().split(",");
            return new Tuple2(test[0], test[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {});

        HBaseOutputFormat hBaseOutputFormat = new HBaseOutputFormat();
        hBaseOutputFormat.setZkQuorum(parameterTool.get("zkQuorum"));
        hBaseOutputFormat.setZkPort(parameterTool.get("zkPort"));

        dataStream.writeUsingOutputFormat(hBaseOutputFormat).name("HBASE Sink");

        // execute program
        env.execute("HBase sink Example");
    }

    private static class HBaseOutputFormat implements OutputFormat<Tuple2<String, String>> {

        private org.apache.hadoop.conf.Configuration conf = null;
        private Table table = null;
        private Connection connection = null;

        private String zkQuorum = null;
        private String zkPort = null;

        public void setZkQuorum(String zkQuorum) {
            this.zkQuorum = zkQuorum;
        }

        public void setZkPort(String zkPort) {
            this.zkPort = zkPort;
        }

        private static final long serialVersionUID = 1L;

        @Override
        public void configure(Configuration parameters) {
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", zkQuorum);
            conf.set("hbase.zookeeper.property.clientPort", (zkPort));

            connection = ConnectionFactory.createConnection(conf);
            table = connection.getTable(TableName.valueOf("flinkTestExample"));
        }

        @Override
        public void writeRecord(Tuple2<String, String> record) throws IOException {
            Put put = new Put(Bytes.toBytes(record.f0));
            put.addColumn(Bytes.toBytes("data"),Bytes.toBytes("test1"), Bytes.toBytes(record.f1));
            table.put(put);
        }

        @Override
        public void close() throws IOException {
            table.close();
            connection.close();
        }

    }
}
