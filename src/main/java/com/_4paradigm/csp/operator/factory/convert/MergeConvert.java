package com._4paradigm.csp.operator.factory.convert;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Collector;

import java.util.Set;

/**
 * 用于将打平的 BO 表内嵌输出
 * eg: {"id": 1, "feat1": "value1", "feat2": "value2"}
 * ->
 * {"id": 1, "feas": {"feat1": "value1", "feat2": "value2"}}
 */
@Slf4j
public class MergeConvert extends RichFlatMapFunction<String, String> {

    public static final String DEFAULT_MERGE_FIELD_DEFAULT_NAME = "feas";
    public static final String DEFAULT_EXCLUDE_MERGE_FIELD = "id";

    // 不用 merge 的 field, 默认 id
    private Set<String> excludeMergeFields;

    private String nestedFieldName;

    public MergeConvert(ParameterTool parameterTool) {
        this.nestedFieldName = parameterTool.get(PropertiesConstants.SINK_MERGE_FIELD, DEFAULT_MERGE_FIELD_DEFAULT_NAME);
        this.excludeMergeFields = Sets.newHashSet(StringUtils.split(parameterTool.get(PropertiesConstants.SINK_MERGE_EXCLUDE_FIELDS, DEFAULT_EXCLUDE_MERGE_FIELD), ","));
        log.info("Merge Convert exclude merge fields [{}], target nestedFieldName [{}]", excludeMergeFields, nestedFieldName);
    }


    @Override
    public void flatMap(String jsonStr, Collector<String> out) {
        if (jsonStr == null || jsonStr.isEmpty()) {
            return;
        }
        JSONObject jsonObject = JSONObject.parseObject(jsonStr);

        JSONObject ans = new JSONObject();
        ans.put(nestedFieldName, new JSONObject());
        jsonObject.forEach((key, value) -> {
            if (excludeMergeFields.contains(key)) {
                ans.put(key, value);
            } else {
                JSONObject featObject = ans.getJSONObject(nestedFieldName);
                featObject.put(key, value);
            }
        });
        out.collect(ans.toJSONString());
    }
}
