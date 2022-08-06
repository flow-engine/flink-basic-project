package com._4paradigm.csp.operator.utils;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.LinkedHashSet;
import java.util.List;

@Slf4j
public class KeyUtils {

    /**
     * 错误行日志记录行数
     */
    private static Long errorLineMaxLog = 100L;

    public static String md5KeysContent(String jsonContent, LinkedHashSet<String> keys) {
        JSONObject jsonObject = validJSON(jsonContent);
        if (jsonObject == null) {
            if (errorLineMaxLog > 0) {
                log.error("Parse Json string error. jsonString [{}]", jsonContent);
                errorLineMaxLog--;
            }
            return "";
        }
        List<String> keyContents = Lists.newArrayList();
        for (String key : keys) {
            Object obj = jsonObject.getOrDefault(key, "");
            keyContents.add(String.valueOf(obj));
        }
        String rawContent = String.join(",", keyContents);
        return MD5Util.md5HexUpperCase(rawContent);
    }

    public static JSONObject validJSON(String jsonContent) {
        if (jsonContent == null || jsonContent.isEmpty()) {
            return null;
        }
        try {
            return JSONObject.parseObject(jsonContent);
        } catch (Exception e) {
            return null;
        }
    }
}
