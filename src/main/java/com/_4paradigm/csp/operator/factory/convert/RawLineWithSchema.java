package com._4paradigm.csp.operator.factory.convert;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SourceType;
import com._4paradigm.csp.operator.factory.convert.parser.AbstractLineParser;
import com._4paradigm.csp.operator.factory.convert.parser.CsvLineParser;
import com._4paradigm.csp.operator.factory.convert.parser.TextLineParser;
import com._4paradigm.csp.operator.utils.DataConvertUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.List;

@Slf4j
public class RawLineWithSchema extends RichFlatMapFunction<String, String> {

    private static final long serialVersionUID = 1L;

    private transient AbstractLineParser lineParser;

    private String headerLine;
    private String[] header;
    private String fieldDelimiter;
    private Boolean firstLineSchema;

    // can be null
    private TypeInformation<?>[] types;

    /**
     * 错误行日志记录行数
     */
    private Long errorLineMaxLog = 100L;

    public RawLineWithSchema(String[] header, boolean firstLineSchema, String fieldDelimiter, TypeInformation<?>[] types) {
        this.header = header;
        this.firstLineSchema = firstLineSchema;
        this.fieldDelimiter = fieldDelimiter;
        // TODO: join
        this.headerLine = String.join(fieldDelimiter, header);
        this.types = types;
        log.info("FirstLineIsSchema [{}], fieldDelimiter [{}], schemaLine [{}]", firstLineSchema, fieldDelimiter, headerLine);
        log.info("Header: [{}]", JSON.toJSONString(this.header));
        log.info("Types: [{}]", JSON.toJSONString(types));
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String sourceTypeStr = parameterTool.get(PropertiesConstants.SOURCE_TYPE);
        SourceType sourceType = SourceType.valueOf(sourceTypeStr.toLowerCase());

        if (SourceType.csv.equals(sourceType)) {
            CSVFormat csvFormat = CSVFormat.RFC4180.withDelimiter(fieldDelimiter.charAt(0)).withEscape('\\');
            this.lineParser = new CsvLineParser(csvFormat);
        } else {
            this.lineParser = new TextLineParser(this.fieldDelimiter);
        }
    }

    /**
     * @param line
     * @param out
     * @throws Exception
     */
    @Override
    public void flatMap(String line, Collector<String> out) throws Exception {
        // skip header
        if (firstLineSchema && line.equals(this.headerLine)) {
            return;
        }

        // 分隔符解析错误
        List<String> data;
        try {
            data = this.lineParser.parse(line);
        } catch (Exception e) {
            if (errorLineMaxLog > 0) {
                log.error("Parse Line error.", e);
                errorLineMaxLog--;
            }
            return;
        }

        // 行数不一致
        if (data.size() != header.length) {
            if (errorLineMaxLog > 0) {
                log.error("Invalid line length (current: {}, expected: {}) for line: [{}]", data.size(), header.length, line);
                errorLineMaxLog--;
            }
            return;
        }

        JSONObject jsonObject = new JSONObject();
        for (int i = 0; i < header.length; i++) {
            String fieldName = header[i];
            String originValue = data.get(i);
            Object value = this.types == null ? originValue : DataConvertUtil.convert(this.types[i], originValue, fieldName);
            jsonObject.put(fieldName, value);
        }
        out.collect(jsonObject.toJSONString());
    }
}
