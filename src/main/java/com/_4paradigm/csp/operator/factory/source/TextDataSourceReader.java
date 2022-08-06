package com._4paradigm.csp.operator.factory.source;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SourceType;
import com._4paradigm.csp.operator.factory.convert.RawLineWithSchema;
import com._4paradigm.csp.operator.utils.ASCIIUtil;
import com._4paradigm.csp.operator.utils.SchemaUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.table.api.TableSchema;

import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

@Slf4j
public class TextDataSourceReader extends AbstractStringStreamReader {
    private Boolean firstLineSchema;
    private String lineDelimiter;
    private String fieldDelimiter;
    private String charset;

    /**
     * 如果用户传递了 schema 就不用读首行了
     * 1. 优先以传递的 schema 为准
     * 2. 如果没有传递 sourceSchema, 则去读首行
     */
    private String firstLine;
    private TableSchema sourceSchema;

    @Override
    public String converterOperatorId() {
        return "source-text-data-converter-id";
    }

    @Override
    public FileProcessingMode getWatchType() {
        return null;
    }

    public TextDataSourceReader(ParameterTool parameterTool) {
        this.firstLineSchema = parameterTool.getBoolean(PropertiesConstants.SOURCE_FIRST_LINE_SCHEMA, false);
        if (SchemaUtil.haveSourceSchema(parameterTool)) {
            this.sourceSchema = SchemaUtil.parse(parameterTool);
            log.info("Source Schema [{}]", sourceSchema);
        }

        this.lineDelimiter = ASCIIUtil.tryToDecode(parameterTool.get(PropertiesConstants.SOURCE_LINE_DELIMITER, "\n"));
        this.fieldDelimiter = ASCIIUtil.tryToDecode(parameterTool.get(PropertiesConstants.SOURCE_FIELD_DELIMITER));

        if (this.fieldDelimiter == null) {
            /**
             * * lineSpliter: "0x0A" ('\n')
             * tsv
             * fieldDelimiter: "0x09" ('\t')
             * csv:
             * fieldDelimiter: "0x2C" (',')
             */
            String sourceTypeStr = parameterTool.get(PropertiesConstants.SOURCE_TYPE);
            SourceType sourceType = SourceType.valueOf(sourceTypeStr.toLowerCase());
            this.fieldDelimiter = SourceType.tsv.equals(sourceType) ? ASCIIUtil.decodeHexString("\t") : ",";
        }

        this.charset = parameterTool.get(PropertiesConstants.SOURCE_CHARSET, StandardCharsets.UTF_8.name());
        log.info("TextDataSource init...\nIsFirstLineSchema [{}], lineDelimiter [{}], fieldDelimiter [{}], charset [{}]",
                firstLineSchema, lineDelimiter, fieldDelimiter, charset);
    }

    @Override
    public FileInputFormat getInputFormat(String sourcePath) {
        Path path = new Path(sourcePath);
        TextInputFormat format = new TextInputFormat(path);
        format.setCharsetName(this.charset);
        // 注意: TextInputFormat.delimiter 是行分隔符
        format.setDelimiter(this.lineDelimiter);
        if (this.sourceSchema == null) {
            this.firstLine = hdfsUtil.readFirstLine(sourcePath, this.lineDelimiter);
            log.info("First line: [{}]", this.firstLine);
        }
        return format;
    }

    @Override
    public DataStream<String> convert(DataStream source) {
        FlatMapFunction convert = this.getConverter();
        return source.flatMap(convert).uid(converterOperatorId());
    }

    private RawLineWithSchema getConverter() {
        if (this.sourceSchema != null) {
            // 用户传递 schema
            log.info("Source schema {}", this.sourceSchema);
            String[] fieldNames = this.sourceSchema.getFieldNames();
            TypeInformation<?>[] types = this.sourceSchema.getFieldTypes();
            return new RawLineWithSchema(fieldNames, this.firstLineSchema, this.fieldDelimiter, types);
        } else {
            // 读首行推断 schema
            log.info("first line: {}", this.firstLine);
            log.info("Is the first line is schema [{}], fieldDelimiter [{}]", this.firstLineSchema, this.fieldDelimiter);
            String[] firstLineSplit = this.firstLine.split(Pattern.quote(fieldDelimiter));
            if (firstLineSchema) {
                return new RawLineWithSchema(firstLineSplit, this.firstLineSchema, this.fieldDelimiter, null);
            } else {
                String[] defaultHeader = new String[firstLineSplit.length];
                for (int i = 0; i < firstLineSplit.length; i++) {
                    defaultHeader[i] = "col_" + i;
                }
                return new RawLineWithSchema(defaultHeader, this.firstLineSchema, this.fieldDelimiter, null);
            }
        }
    }
}
