package com._4paradigm.csp.operator.factory.convert;

import com._4paradigm.csp.operator.utils.DataConvertUtil;
import com.alibaba.fastjson.JSONObject;
import java.util.Arrays;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * JSON String 转换成 Row
 */
@Slf4j
public class RawJsonLineConvert extends RichFlatMapFunction<String, Row> {

    private String[] header;

    private TypeInformation<?>[] types;

    /**
     * 错误行日志记录行数
     */
    private Long errorLineMaxLog = 100L;

    private Long warnLineMaxLog = 100L;

    public RawJsonLineConvert(String[] header, TypeInformation[] types) {
        this.header = header;
        this.types = types;
        log.info("Header: {}", Arrays.toString(this.header));
        log.info("Types: {}", Arrays.toString(this.types));
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
    }

    /**
     * @param jsonString
     * @param out
     */
    @Override
    public void flatMap(String jsonString, Collector<Row> out) {
        log.info("message: {}", jsonString);
        JSONObject jsonObject;

        try {
            jsonObject = JSONObject.parseObject(jsonString);
        } catch (Exception e) {
            if (errorLineMaxLog > 0) {
                log.error("Parse Json string error. jsonString [{}]", jsonString, e);
                errorLineMaxLog--;
            }
            return;
        }

        int pos = 0;
        Row row = new Row(header.length);
        for (int i = 0; i < header.length; i++) {
            String columnName = header[i];
            TypeInformation type = types[i];
            Object value = jsonObject.get(columnName);
            if (value == null) {
                if (warnLineMaxLog > 0) {
                    log.warn("Null value of columnName [{}] in json [{}]", columnName, jsonString);
                    warnLineMaxLog--;
                }
                row.setField(pos++, null);
            } else {
                log.info("Field: [{}], Type: [{}], Value: [{}]", columnName, type, value);
                row.setField(pos++, DataConvertUtil.convert(type, value, columnName));
            }
        }
        out.collect(row);
    }
}
