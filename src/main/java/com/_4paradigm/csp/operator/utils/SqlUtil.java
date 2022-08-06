package com._4paradigm.csp.operator.utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SqlUtil {

    private static final String CONVERTER_SQL_JSON_TO_STRING_ID = "converter-sql-json-to-string-id";

//    public static DataStream<String> executeSql(DataStream dataStreamSource, final StreamExecutionEnvironment env, final ParameterTool parameterTool) {
//        // SQL and convert
//        // stream.sql/stream.schema
//        String tableName = parameterTool.get(PropertiesConstants.STREAM_SQL_TABLE, "t1");
//        String sql = parameterTool.get(PropertiesConstants.STREAM_SQL, "select * from t1");
//        TableSchema tableSchema = SchemaUtil.parse(parameterTool);
//        List<String> fields = Lists.newArrayList(tableSchema.getFieldNames());
//
//        String sourceTypeStr = parameterTool.getRequired(PropertiesConstants.SOURCE_TYPE);
//        SourceType sourceType = SourceType.valueOf(sourceTypeStr.toLowerCase());
//        DataStream sqlDataSource = dataStreamSource;
//        if (!SourceType.kafka.equals(sourceType)) {
//            // 数据源不是 kafka 需要将 JsonString 转换成带 schema 信息的 JsonRow 才能执行 sql
//            KafkaSourceReader kafkaSourceReader = new KafkaSourceReader();
//            sqlDataSource = kafkaSourceReader.convert(tableSchema, dataStreamSource);
//        }
//
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        tableEnv.registerFunction(ArraySplit.NAME, new ArraySplit());
//        tableEnv.createTemporaryView(tableName, sqlDataSource, String.join(",", fields));
//
//        Table queryTable = tableEnv.sqlQuery(sql);
//        TableSchema queryTableSchema = queryTable.getSchema();
//        log.info("Query Table [{}] Schema: {}", tableName, queryTableSchema);
//
//        // 转换成带 schema 信息的 String 输出
//        DataStream<Row> outputStream = tableEnv.toAppendStream(queryTable, Row.class);
//        RowWithSchemaConvert convert = new RowWithSchemaConvert(queryTableSchema.getFieldNames());
//        SingleOutputStreamOperator<String> ans = outputStream.flatMap(convert).uid(CONVERTER_SQL_JSON_TO_STRING_ID);
////        ans.print();
//        return ans;
//    }
}
