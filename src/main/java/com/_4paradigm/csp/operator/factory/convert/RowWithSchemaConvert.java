package com._4paradigm.csp.operator.factory.convert;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

@Slf4j
public class RowWithSchemaConvert extends RichFlatMapFunction<Row, String> {

    private static final long serialVersionUID = 1L;

    private String[] header;

    public RowWithSchemaConvert(String[] header) {
        this.header = header;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
    }

    @Override
    public void flatMap(Row row, Collector<String> out) {
        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < header.length; i++) {
            jsonObject.put(header[i], row.getField(i));
        }
        out.collect(jsonObject.toJSONString());
    }
}
