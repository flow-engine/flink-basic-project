package com._4paradigm.csp.operator.utils;


import org.apache.flink.api.common.typeinfo.Types;
import org.junit.Test;

import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import static org.assertj.core.api.Assertions.assertThat;

public class RawJsonLineConvertTest {

    static SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd");

    @Test
    public void testCommonConvert() throws ParseException {
        // case 1: 正常
        assertThat(DataConvertUtil.convert(Types.BOOLEAN, true, null)).isEqualTo(true);
        assertThat(DataConvertUtil.convert(Types.SHORT, 1, null)).isEqualTo((short) 1);
        assertThat(DataConvertUtil.convert(Types.INT, 100, null)).isEqualTo(100);
        assertThat(DataConvertUtil.convert(Types.LONG, 100_000_000L, null)).isEqualTo(100_000_000L);
        assertThat(DataConvertUtil.convert(Types.FLOAT, 1.0, null)).isEqualTo((float) 1.0);
        assertThat(DataConvertUtil.convert(Types.DOUBLE, 2.0, null)).isEqualTo(2.0);
//        assertThat(((Timestamp) DataConvertUtil.convert(Types.SQL_TIMESTAMP, "2020-04-03 15:32:09.999", null)).getTime()).isEqualTo(1585899129999L);
        assertThat(DataConvertUtil.convert(Types.SQL_DATE, "2020-04-03", null)).isEqualTo(sf.parse("2020-04-03"));
        assertThat(DataConvertUtil.convert(Types.SQL_DATE, "2020-04-03", null)).isInstanceOf(java.sql.Date.class);
        assertThat(DataConvertUtil.convert(Types.STRING, "string", null)).isEqualTo("string");
    }

    @Test
    public void testOverflow() {
        // case 2: 溢出相关 -> null
        assertThat(DataConvertUtil.convert(Types.INT, 99999999999999999L, null)).isNull();
    }

    @Test
    public void testTimestamp() {
        // case 3: 时间相关
        // Timestamp 支持 13 位的 Long, 支持 yyyy-MM-dd HH:mm:ss, yyyy-MM-dd HH:mm:ss.SSS
        assertThat(((Timestamp) DataConvertUtil.convert(Types.SQL_TIMESTAMP, 1585899129989L, null)).getTime()).isEqualTo(1585899129989L);
//        assertThat(((Timestamp) DataConvertUtil.convert(Types.SQL_TIMESTAMP, "2020-04-03 15:32:09.99", null)).getTime()).isEqualTo(1585899129099L);
//        assertThat(((Timestamp) DataConvertUtil.convert(Types.SQL_TIMESTAMP, "2020-04-03 15:32:09.999", null)).getTime()).isEqualTo(1585899129999L);
        // Date 只支持 yyyy-MM-dd
    }
}
