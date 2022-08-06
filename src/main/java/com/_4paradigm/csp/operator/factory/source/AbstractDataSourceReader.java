package com._4paradigm.csp.operator.factory.source;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.utils.HdfsUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.io.FilePathFilter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

import java.util.List;

@Slf4j
public abstract class AbstractDataSourceReader {

    public abstract FileInputFormat getInputFormat(String path);

    public abstract TypeInformation getTypeInformation();

    // TODO: 调用 convert 之前需要先调用 getInputFormat 获取 schema 信息
    public abstract DataStream convert(DataStream source);

    // 建议给每一个有状态的算子分配 operatorId
    // https://flink.sojb.cn/ops/state/savepoints.html#should-i-assign-ids-to-all-operators-in-my-job
    public abstract String converterOperatorId();

    public abstract FileProcessingMode getWatchType();

    private static final String READ_FS_SOURCE_OPERATOR_ID = "source-read-fs-id";

    List<String> fields;

    HdfsUtil hdfsUtil;

    public DataStream readDataSourceStream(final StreamExecutionEnvironment env, final ParameterTool parameterTool) {

        this.hdfsUtil = HdfsUtil.buildUtil(parameterTool);

        String sourcePath = parameterTool.getRequired(PropertiesConstants.SOURCE_PATH);
        log.info("Source [{}]", sourcePath);

        FileProcessingMode watchType = FileProcessingMode.valueOf(parameterTool.get(PropertiesConstants.SOURCE_WATCH_TYPE, FileProcessingMode.PROCESS_CONTINUOUSLY.name()));
        long watchInterval = parameterTool.getLong(PropertiesConstants.SOURCE_WATCH_INTERVAL, 60_000L);
        log.info("Source watchType [{}], watchInterval [{}]'ms", watchType, watchInterval);

        // read
        FileInputFormat inputFormat = getInputFormat(sourcePath);
        TypeInformation typeInformation = getTypeInformation();
        inputFormat.setFilesFilter(FilePathFilter.createDefaultFilter());
        DataStreamSource source = env.readFile(inputFormat, sourcePath, getWatchType(), watchInterval, typeInformation);
        source.uid(READ_FS_SOURCE_OPERATOR_ID);

        // convert
        return this.convert(source);
    }
}
