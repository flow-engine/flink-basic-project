package com._4paradigm.csp.operator.utils;

import com._4paradigm.csp.operator.enums.CheckPointType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_DIR;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_ENABLE;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_INTERVAL;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_MIN_PAUSE;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_MODE;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_TIMEOUT;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUM;
import static com._4paradigm.csp.operator.constant.PropertiesConstants.STREAM_CHECKPOINT_TYPE;


/**
 * https://wiki.4paradigm.com/display/PlatformRD/job+failure+and+recovery#jobfailureandrecovery-Checkpointing
 */
@Slf4j
public class CheckPointUtil {

    public static StreamExecutionEnvironment setCheckpointConfig(final StreamExecutionEnvironment env, final ParameterTool parameterTool) throws Exception {
        if (StringUtils.isNotEmpty(parameterTool.get(STREAM_CHECKPOINT_ENABLE)) && !parameterTool.getBoolean(STREAM_CHECKPOINT_ENABLE)) {
            return env;
        }

        StateBackend stateBackend = createStateBackend(parameterTool);
        env.setStateBackend(stateBackend);

        // Checkpoint 语义默认为 AT_LEAST_ONCE
        CheckpointingMode checkpointingMode = CheckpointingMode.valueOf(parameterTool.get(STREAM_CHECKPOINT_MODE, CheckpointingMode.AT_LEAST_ONCE.name()).toUpperCase());
        env.getCheckpointConfig().setCheckpointingMode(checkpointingMode);

        //设置 checkpoint 周期时间, 默认: at least once 每一分钟进行一次; exactly once 每 30s 进行一次
        long defaultInterval = CheckpointingMode.AT_LEAST_ONCE.equals(checkpointingMode) ? 60_000 : 30_000;
        env.enableCheckpointing(parameterTool.getLong(STREAM_CHECKPOINT_INTERVAL, defaultInterval));
        // 设置 checkpoint 超时时间 十分钟, 否则会被丢弃
        env.getCheckpointConfig().setCheckpointTimeout(parameterTool.getLong(STREAM_CHECKPOINT_TIMEOUT, 600_000));
        // 作业最多允许 Checkpoint 失败 10 次
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(parameterTool.getInt(STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUM, 10));
        // 设置 checkpoint 的并发度为 1(同一时间, 只允许有 1 个 CheckPoint 在发生)
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 两次 checkpoint 最小间隔 1s
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(parameterTool.getLong(STREAM_CHECKPOINT_MIN_PAUSE, 1000));
        // 当 Flink 任务取消时，默认保留外部保存的 CheckPoint 信息
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setPreferCheckpointForRecovery(true);
        return env;
    }

    private static StateBackend createStateBackend(final ParameterTool parameterTool) throws IOException, URISyntaxException {
        String checkPointTypeStr = parameterTool.get(STREAM_CHECKPOINT_TYPE, CheckPointType.rocksdb.name()).toLowerCase();
        String checkPointPath = parameterTool.get(STREAM_CHECKPOINT_DIR, "hdfs:///tmp/csp/flink/checkpoints");
        log.info("CheckPoint type [{}], path [{}]", checkPointTypeStr, checkPointPath);
        CheckPointType checkPointType = CheckPointType.valueOf(checkPointTypeStr);

        if (CheckPointType.rocksdb.equals(checkPointType)) {
            // 1. 默认 rocks db 增量
            RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend(checkPointPath, true);
            // 设置为机械硬盘+内存模式
//            rocksDBStateBackend.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED_HIGH_MEM);
            return rocksDBStateBackend;
        } else if (CheckPointType.fs.equals(checkPointType)) {
            // 2. fs 存储
            StateBackend stateBackend = new FsStateBackend(new URI(checkPointPath), 0);
            return stateBackend;
        } else if (CheckPointType.memory.equals(checkPointType)) {
            // 3. state 存放在内存中，默认是 5M
            StateBackend stateBackend = new MemoryStateBackend(5 * 1024 * 1024 * 100);
            return stateBackend;
        }
        throw new RuntimeException(String.format("Not support CheckPoint type [%s], only support [rocksdb, fs, memory]"));
    }
}
