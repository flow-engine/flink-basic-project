package com._4paradigm.csp.operator.utils;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.TimeZone;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;

@Slf4j
public class DataConvertUtil {

    public static FastDateFormat originalDateFormat = FastDateFormat.getInstance("yyyy-MM-dd");

    private static FastDateFormat prophetDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
    private static FastDateFormat prophetDateFormatExtra = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss.SSS");

    public static final ObjectMapper jsonMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .configure(JsonParser.Feature.ALLOW_COMMENTS, true)
            .setTimeZone(TimeZone.getTimeZone("GMT+8"));

    public static Object convert(TypeInformation type, Object value, String fieldName) {
        if (value == null) {
            return value;
        }
        try {
            switch (type.getTypeClass().getSimpleName()) {
                case "Boolean":
                    return Boolean.valueOf(String.valueOf(value));
                case "Short":
                    return Short.valueOf(String.valueOf(value));
                case "Integer":
                    return Integer.valueOf(String.valueOf(value));
                case "Long":
                    return Long.valueOf(String.valueOf(value));
                case "Float":
                    return Float.valueOf(String.valueOf(value));
                case "Double":
                    return Double.valueOf(String.valueOf(value));
                // TODO: 时间相关兼容其它格式
                case "Timestamp":
                    // 13 位
                    Long time;
                    try {
                        time = Long.valueOf(String.valueOf(value));
                    } catch (NumberFormatException e) {
                        try {
                            time = prophetDateFormatExtra.parse(String.valueOf(value)).getTime();
                        } catch (ParseException e1) {
                            time = prophetDateFormat.parse(String.valueOf(value)).getTime();
                        }
                    }
                    if (time != null) {
                        return new Timestamp(time);
                    }
                case "Date":
                     return Date.valueOf(String.valueOf(value));
                default:
                    // String, List, Map
                    return String.valueOf(value);
            }
        } catch (Exception e) {
            log.error("Field [{}]'s Data [{}] convert to type [{}] error", fieldName, value, type, e);
            return null;
        }
    }
}
