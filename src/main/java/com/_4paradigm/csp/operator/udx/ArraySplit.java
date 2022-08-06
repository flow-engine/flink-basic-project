package com._4paradigm.csp.operator.udx;

import com._4paradigm.csp.operator.factory.convert.parser.TextLineParser;
import com.google.common.collect.Lists;
import org.apache.flink.table.functions.ScalarFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * https://wiki.4paradigm.com/pages/viewpage.action?pageId=76329580
 */
public class ArraySplit extends ScalarFunction {
    public static final String NAME = "arraySplit";

    private String separator = ",";

    public List<String> eval(String str) {
        return eval(str, separator);
    }

    public List<String> eval(String str, String separator) {
        if (separator != null && !separator.isEmpty()) {
            this.separator = separator;
        }
        if (str != null && !str.isEmpty()) {
            return Lists.newArrayList(str.split(TextLineParser.escapeSpecialRegexChars(this.separator), -1));
        }
        return new ArrayList<>();
    }
}
