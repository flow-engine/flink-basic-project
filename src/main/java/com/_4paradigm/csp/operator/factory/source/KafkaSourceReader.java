package com._4paradigm.csp.operator.factory.source;

import com._4paradigm.csp.operator.factory.KafkaFactory;
import com._4paradigm.csp.operator.factory.convert.RawJsonLineConvert;
import com._4paradigm.csp.operator.utils.SchemaUtil;
import com._4paradigm.pdms.telamon.model.CommonSchemaTerm;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.types.Row;

/**
 * 目前只支持一层嵌套的 json
 * <p>
 * * json: {"id": 1, "name": "n1"}
 */
public class KafkaSourceReader extends AbstractRowStreamReader {

    private static final String READ_KAFKA_SOURCE_OPERATOR_ID = "source-read-kafka-id";

    private static final String JSON_CONVERTER_ID = "converter-json-data-to-row-data-id";

    @Override
    public FileInputFormat getInputFormat(String path) {
        throw new RuntimeException("Don't need");
    }

    @Override
    public FileProcessingMode getWatchType() {
        return FileProcessingMode.PROCESS_CONTINUOUSLY;
    }

    @Override
    public DataStream readDataSourceStream(StreamExecutionEnvironment env, ParameterTool parameterTool) {
        FlinkKafkaConsumerBase<String> consumer = KafkaFactory.consumer(parameterTool, new HashMap<>());
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);
        dataStreamSource.uid(READ_KAFKA_SOURCE_OPERATOR_ID);
//        dataStreamSource.print();

        // String -> json -> row(withSchemaInfo)
        TableSchema tableSchema = SchemaUtil.parse(parameterTool);

        return convert(tableSchema, dataStreamSource);
    }

    public DataStream convert(TableSchema tableSchema, DataStream dataStreamSource) {
        String[] fieldNames = tableSchema.getFieldNames();
        TypeInformation<?>[] types = tableSchema.getFieldTypes();

        RawJsonLineConvert convert = new RawJsonLineConvert(fieldNames, types);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);
        // 尽量在调用处设置 uid, 以免重复
        return dataStreamSource.flatMap(convert).returns(rowTypeInfo).uid(JSON_CONVERTER_ID);
    }

    public DataStream readDataSourceStreamWithSchema(StreamExecutionEnvironment env,
            ParameterTool parameterTool,
            List<CommonSchemaTerm> groupSchemas, Map<String, String> sourceParameter) {
        FlinkKafkaConsumerBase<String> consumer = KafkaFactory.consumer(parameterTool, sourceParameter);
        DataStreamSource<String> dataStreamSource = env.addSource(consumer);

        // String -> json -> row(withSchemaInfo)
        TableSchema tableSchema = SchemaUtil.parseFromPdms(groupSchemas);

        String[] fieldNames = tableSchema.getFieldNames();
        TypeInformation<?>[] types = tableSchema.getFieldTypes();

        RawJsonLineConvert convert = new RawJsonLineConvert(fieldNames, types);
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fieldNames);

        return dataStreamSource.flatMap(convert).returns(rowTypeInfo);
    }

}
