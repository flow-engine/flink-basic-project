package com._4paradigm.csp.operator.factory.convert.parser;

import com.google.common.collect.Lists;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.regex.Pattern;

@Slf4j
@NoArgsConstructor
@AllArgsConstructor
public class TextLineParser extends AbstractLineParser {
    private static Pattern SPECIAL_REGEX_CHARS = Pattern.compile("[{}()\\[\\].+*?^$\\\\|]");

    private String fieldDelimiter;

    @Override
    public List<String> parse(String line) {
        return Lists.newArrayList(line.split(escapeSpecialRegexChars(fieldDelimiter), -1));
    }

    public static String escapeSpecialRegexChars(String str) {
        return SPECIAL_REGEX_CHARS.matcher(str).replaceAll("\\\\$0");
    }
}
