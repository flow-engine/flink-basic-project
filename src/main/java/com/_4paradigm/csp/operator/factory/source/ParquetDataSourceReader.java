package com._4paradigm.csp.operator.factory.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.parquet.ParquetRowInputFormat;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.stream.Collectors;

/**
 * https://help.semmle.com/wiki/display/JAVA/Non-serializable+field
 */
@Slf4j
public class ParquetDataSourceReader extends AbstractRowStreamReader {

    @Override
    public FileInputFormat getInputFormat(String sourcePath) {
        MessageType parquetSchema = hdfsUtil.readParquetSchema(sourcePath);
        this.fields = parquetSchema.getFields().stream().map(Type::getName).collect(Collectors.toList());
        log.info("Source [{}], schema [{}]", sourcePath, parquetSchema);
        return new ParquetRowInputFormat(new Path(sourcePath), parquetSchema);
    }

    @Override
    public FileProcessingMode getWatchType() {
        return FileProcessingMode.PROCESS_ONCE;
    }

}
