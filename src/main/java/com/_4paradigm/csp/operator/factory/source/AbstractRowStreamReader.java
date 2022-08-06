package com._4paradigm.csp.operator.factory.source;

import com._4paradigm.csp.operator.factory.convert.RowWithSchemaConvert;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.types.Row;

@Slf4j
public abstract class AbstractRowStreamReader extends AbstractDataSourceReader {

    private static final String CONVERTER_OP_ID = "converter-row-data-to-jsonString-id";

    @Override
    public TypeInformation getTypeInformation() {
        return RowTypeInfo.of(Row.class);
    }

    @Override
    public String converterOperatorId() {
        return CONVERTER_OP_ID;
    }

    @Override
    public DataStream<String> convert(DataStream source) {
        if (this.fields == null) {
            throw new RuntimeException("The schema of AbstractRowStreamReader is null");
        }

        RowWithSchemaConvert converter = new RowWithSchemaConvert(this.fields.toArray(new String[this.fields.size()]));
        return source.flatMap(converter).uid(converterOperatorId());
    }

}
