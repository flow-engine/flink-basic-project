package com._4paradigm.csp.operator.utils;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.pdms.telamon.model.CommonSchemaTerm;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.table.api.TableSchema;

import java.util.Map;

/**
 * 支持的类型: https://gitlab.4pd.io/PHT3/telamon/blob/develop/interface/src/main/java/com/_4paradigm/pdms/telamon/enumeration/TableSchemaType.java
 * 相关转换关系: https://wiki.4paradigm.com/pages/viewpage.action?pageId=74764297
 */
@Slf4j
public class SchemaUtil {

    public static TableSchema parse(final ParameterTool parameterTool) {
        String schema = parameterTool.get(PropertiesConstants.STREAM_SQL_SCHEMA_JSON);
        if (schema != null) {
            return parseJson(schema);
        }
        schema = parameterTool.get(PropertiesConstants.STREAM_SQL_SCHEMA);
        if (schema != null) {
            return parseStruct(schema);
        }
        throw new RuntimeException("Schema can't be null, please set 'stream.sql.schema.json' or 'stream.sql.schema'");
    }

    public static boolean haveSourceSchema(final ParameterTool parameterTool) {
        return StringUtils.isNotEmpty(parameterTool.get(PropertiesConstants.STREAM_SQL_SCHEMA_JSON)) ||
                StringUtils.isNotEmpty(parameterTool.get(PropertiesConstants.STREAM_SQL_SCHEMA));
    }

    static TableSchema parseJson(String jsonSchema) {
        log.info("Raw json schema: [{}]", jsonSchema);
        // TODO: 因为 schema 也被用于 csv 等文本文件的 schema, 所以必须有序解析
        JSONObject schemaObject = JSON.parseObject(jsonSchema, Feature.OrderedField);

        TableSchema.Builder builder = new TableSchema.Builder();
        for (Map.Entry<String, Object> entry : schemaObject.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
            builder.field(entry.getKey(), fromSDPType(String.valueOf(entry.getValue())));
        }

        TableSchema tableSchema = builder.build();
        log.info("Parse schema: [{}]", tableSchema);
        return tableSchema;
    }

    // struct<id:BigInt,name:String>
    static TableSchema parseStruct(String rawSchema) {
        log.info("Raw struct schema: [{}]", rawSchema);
        String substring = rawSchema.trim().substring(7, rawSchema.length() - 1);
        String[] split = substring.split(",");
        TableSchema.Builder builder = new TableSchema.Builder();
        for (int i = 0; i < split.length; i++) {
            String[] columnAndType = split[i].split(":");
            builder.field(columnAndType[0], fromSDPType(columnAndType[1]));
        }
        TableSchema tableSchema = builder.build();
        log.info("Parse schema: [{}]", tableSchema);
        return tableSchema;
    }

    public static TableSchema parseFromPdms(List<CommonSchemaTerm> schemaTermList) {
        log.info("Raw json schema: [{}]", schemaTermList.toString());

        TableSchema.Builder builder = new TableSchema.Builder();
        for (CommonSchemaTerm item: schemaTermList) {
            System.out.println(item.getName() + ":" + item.getType());
            builder.field(item.getName(), fromSDPType(item.getType()));
        }
        TableSchema tableSchema = builder.build();
        log.info("Parse schema: [{}]", tableSchema);
        return tableSchema;
    }

    static TypeInformation fromSDPType(String type) {
        switch (type) {
            case "Boolean":
                return Types.BOOLEAN;
            case "SmallInt":
                return Types.SHORT;
            case "Int":
                return Types.INT;
            case "BigInt":
                return Types.LONG;
            case "Float":
                return Types.FLOAT;
            case "Double":
            case "Decimal":
                return Types.DOUBLE;
            case "Timestamp":
                return Types.SQL_TIMESTAMP;
            case "Date":
                return Types.SQL_DATE;
            default:
                // String, List, Map
                return Types.STRING;
        }
    }

    public static int forSqlFromPdms(String type) {
        switch (type) {
            case "Boolean":
                return java.sql.Types.BOOLEAN;
            case "SmallInt":
                return java.sql.Types.INTEGER;
            case "Int":
                return java.sql.Types.INTEGER;
            case "BigInt":
                return java.sql.Types.BIGINT;
            default:
                // String, List, Map
                return java.sql.Types.VARCHAR;
        }
    }

}
