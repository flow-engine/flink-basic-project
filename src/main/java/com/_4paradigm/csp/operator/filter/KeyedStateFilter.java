package com._4paradigm.csp.operator.filter;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.utils.KeyUtils;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/stream/state/state.html
 * 去重算子
 * key: 为消费的 JSONObject 一组 key 对应 value String 的 md5 值
 */
@Slf4j
public class KeyedStateFilter extends RichFilterFunction<String> {
    // 使用该 ValueState 来标识当前 Key 是否之前存在过
    private ValueState<Boolean> isExist;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        ParameterTool parameterTool = (ParameterTool) getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        ValueStateDescriptor<Boolean> keyedStateDuplicated =
                new ValueStateDescriptor<>("KeyedStateDeduplication",
                        TypeInformation.of(new TypeHint<Boolean>() {
                        }));
        // 状态 TTL 相关配置，过期时间默认设定为 1 小时
        long ttl = parameterTool.getLong(PropertiesConstants.UNIQUE_DATA_TTL_IN_MINUTE, 60L);
        StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(Time.minutes(ttl))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(
                        StateTtlConfig.StateVisibility.NeverReturnExpired)
                .cleanupInRocksdbCompactFilter(50000000L)
                .build();
        // 开启 TTL
        keyedStateDuplicated.enableTimeToLive(ttlConfig);
        // 从状态后端恢复状态
        isExist = getRuntimeContext().getState(keyedStateDuplicated);
    }

    @Override
    public boolean filter(String str) throws Exception {
        if (str == null || str.isEmpty()) {
            // 过滤异常值
            return false;
        }
        log.debug("input: {}", str);
        if (null == isExist.value()) {
            // key 第一次存在
            log.debug("first: {}", str);
            isExist.update(true);
            JSONObject object = KeyUtils.validJSON(str);
            if (object == null) {
                // 过滤 key 为 "" 对应的非法值
                return false;
            }
            return true;
        }
        log.debug("duplicate: {}", str);
        return false;
    }
}
