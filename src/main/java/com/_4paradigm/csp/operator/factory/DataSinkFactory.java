package com._4paradigm.csp.operator.factory;

import java.util.HashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

@Slf4j
public class DataSinkFactory {
    public static SinkFunction sink(final ParameterTool parameterTool) {
        return KafkaFactory.sink(parameterTool,new HashMap<>());
    }
}
