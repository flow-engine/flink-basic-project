package com._4paradigm.csp.operator.factory;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SourceType;
import com._4paradigm.csp.operator.factory.source.AbstractDataSourceReader;
import com._4paradigm.csp.operator.factory.source.KafkaSourceReader;
import com._4paradigm.csp.operator.factory.source.OrcDataSourceReader;
import com._4paradigm.csp.operator.factory.source.ParquetDataSourceReader;
import com._4paradigm.csp.operator.factory.source.TextDataSourceReader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@Slf4j
public class DataSourceFactory {

    public static DataStream readDataSource(final StreamExecutionEnvironment env, final ParameterTool parameterTool) {
        AbstractDataSourceReader sourceReader = getSourceReader(parameterTool);
        return sourceReader.readDataSourceStream(env, parameterTool);
    }

    private static AbstractDataSourceReader getSourceReader(final ParameterTool parameterTool) {
        String sourceTypeStr = parameterTool.getRequired(PropertiesConstants.SOURCE_TYPE);
        log.info("DataSource Type [{}], DataSource Path [{}]", sourceTypeStr);

        SourceType sourceType = SourceType.valueOf(sourceTypeStr.toLowerCase());

        switch (sourceType) {
            case parquet:
                return new ParquetDataSourceReader();
            case csv:
            case txt:
            case tsv:
                //source txt/csv/tsv need some other info
                return new TextDataSourceReader(parameterTool);
            case orc:
                return new OrcDataSourceReader();
            case kafka:
                return new KafkaSourceReader();
            default:
                throw new RuntimeException(String.format("Not supported sourceType: %s", sourceType));
        }
    }
}
