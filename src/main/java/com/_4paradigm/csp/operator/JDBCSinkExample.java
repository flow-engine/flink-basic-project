package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import java.io.IOException;
import java.sql.Types;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
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

public class JDBCSinkExample {

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

        DataStream<Row> rows = dataStream.map((MapFunction<Tuple2<String, String>, Row>) aCase -> {
            Row row = new Row(2); // our prepared statement has 2 parameters
            row.setField(0, aCase.f0); //first parameter is case ID
            row.setField(1, aCase.f1); //second paramater is tracehash
            return row;
        });

        String url = parameterTool.get(PropertiesConstants.SINK_DB_URL);
        String user = parameterTool.get(PropertiesConstants.SINK_DB_USERNAME);
        String password = parameterTool.get(PropertiesConstants.SINK_DB_PASSWORD);
        String table = parameterTool.get(PropertiesConstants.SINK_DB_TABLE);

        JDBCOutputFormat jdbcOutput = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername("com.mysql.cj.jdbc.Driver")
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(password)
                .setSqlTypes(new int[] { Types.VARCHAR, Types.VARCHAR })
                .setQuery(String.format("INSERT INTO %s (`col_1`, `automl_id`) VALUES (?, ?)", table))
                .setBatchInterval(100)
                .finish();

        rows.writeUsingOutputFormat(jdbcOutput).name("JDBC Sink");

        // execute program
        env.execute("JDBC sink Example");
    }


}
