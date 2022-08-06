package com._4paradigm.csp.operator.factory.source;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.orc.OrcRowInputFormat;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.hadoop.conf.Configuration;
import org.apache.orc.TypeDescription;

@Slf4j
public class OrcDataSourceReader extends AbstractRowStreamReader {

    @Override
    public FileInputFormat getInputFormat(String sourcePath) {
        TypeDescription typeDescription = hdfsUtil.readOrcSchema(sourcePath);
        this.fields = typeDescription.getFieldNames();
        String orcSchema = typeDescription.toString();
        log.info("Source [{}], schema [{}]", sourcePath, orcSchema);
        return new OrcRowInputFormat(sourcePath, orcSchema, new Configuration());
    }

    @Override
    public FileProcessingMode getWatchType() {
        return FileProcessingMode.PROCESS_ONCE;
    }
}
