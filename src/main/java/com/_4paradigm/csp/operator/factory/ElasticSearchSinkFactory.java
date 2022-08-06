package com._4paradigm.csp.operator.factory;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SyncEsType;
import com._4paradigm.csp.operator.sink.Elasticsearch6SinkFunction;
import com._4paradigm.csp.operator.sink.Elasticsearch6UpsertSinkFunction;
import com._4paradigm.csp.operator.utils.ESSinkUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.ExceptionUtils;
import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.net.SocketTimeoutException;
import java.util.List;

/**
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/elasticsearch.html
 */
public class ElasticSearchSinkFactory {

    public static ElasticsearchSink sink(final ParameterTool parameterTool) throws Exception {
        List<HttpHost> esHosts = ESSinkUtil.getEsAddresses(parameterTool.getRequired(PropertiesConstants.SINK_ES_HOST));
        String index = parameterTool.getRequired(PropertiesConstants.SINK_ES_INDEX);
        String type = parameterTool.getRequired(PropertiesConstants.SINK_ES_TYPE);
        SyncEsType syncEsType = SyncEsType.valueOf(parameterTool.getRequired(PropertiesConstants.SINK_ES_SYNC_TYPE).toLowerCase());
        String uniqueColumn = parameterTool.getRequired(PropertiesConstants.SINK_ES_UNIQUE_COLUMN);

        // can be null
        String versionColumn = parameterTool.get(PropertiesConstants.SINK_ES_VERSION_COLUMN, PropertiesConstants.SINK_ES_VERSION_COLUMN_DEFAULT);
        Long versionValue = parameterTool.getLong(PropertiesConstants.SINK_ES_VERSION);

        ElasticsearchSinkFunction<?> elasticsearch6SinkFunction;
        if (SyncEsType.full.equals(syncEsType)) {
            elasticsearch6SinkFunction = new Elasticsearch6SinkFunction(index, type, uniqueColumn, versionColumn, versionValue);
        } else {
            int retryOnConflict = parameterTool.getInt(PropertiesConstants.SINK_ES_UPDATE_RETRY_ON_CONFLICT, 5);
            elasticsearch6SinkFunction = new Elasticsearch6UpsertSinkFunction(index, type, uniqueColumn, syncEsType, versionColumn, versionValue, retryOnConflict);
        }

        ElasticsearchSink.Builder esSinkBuilder = new ElasticsearchSink.Builder<>(
                esHosts, elasticsearch6SinkFunction);

        addConfig(esSinkBuilder, parameterTool);

        esSinkBuilder.setFailureHandler(new ActionRequestFailureHandlerImp());

        return esSinkBuilder.build();
    }

    static void addConfig(final ElasticsearchSink.Builder builder, final ParameterTool parameterTool) {
        /**
         * bulk.flush.max.actions: 默认1000。每个Bulk请求，最大缓冲Action个数。
         * bulk.flush.max.size.mb: 默认5mb。每个Bulk请求，最大缓冲的Action大小。
         * bulk.flush.interval.ms: 默认为空,单位毫秒。Bulk刷新间隔。不论Action个数或Action大小如何设置，到刷新间隔了，就会刷新缓冲，发起Bulk请求。
         */
        builder.setBulkFlushMaxActions(parameterTool.getInt("sink.es.bulk.flush.max.actions", 1000));
        builder.setBulkFlushMaxSizeMb(parameterTool.getInt("sink.es.bulk.flush.max.size.mb", 5));
        builder.setBulkFlushInterval(parameterTool.getLong("sink.es.bulk.flush.interval.ms", 60_000)); // default 1min

        /**
         * 延时重试策略:
         * bulk.flush.backoff.enable: 延迟重试是否启用。
         * bulk.flush.backoff.type: 延迟重试类型，CONSTANT(固定间隔)或EXPONENTIAL(指数级间隔)。
         * bulk.flush.backoff.delay: 延迟重试间隔。对于CONSTANT类型，此值为每次重试间的间隔;对于EXPONENTIAL，此值为初始延迟。
         * bulk.flush.backoff.retries: 延迟重试次数。
         */
        builder.setBulkFlushBackoff(parameterTool.getBoolean("sink.es.bulk.flush.backoff.enable", true));
        String flushBackOffTypeName = parameterTool.get("sink.bulk.flush.backoff.type", ElasticsearchSinkBase.FlushBackoffType.CONSTANT.name()).toUpperCase();
        ElasticsearchSinkBase.FlushBackoffType flushBackOffType = ElasticsearchSinkBase.FlushBackoffType.valueOf(flushBackOffTypeName);
        builder.setBulkFlushBackoffType(flushBackOffType);
        builder.setBulkFlushBackoffDelay(parameterTool.getLong("sink.es.bulk.flush.backoff.delay", 60_000));
        builder.setBulkFlushBackoffRetries(parameterTool.getInt("sink.es.bulk.flush.backoff.delay", 5));
    }

    @Slf4j
    static class ActionRequestFailureHandlerImp implements ActionRequestFailureHandler {

        @Override
        public void onFailure(ActionRequest action,
                              Throwable failure,
                              int restStatusCode,
                              RequestIndexer indexer) throws Throwable {

            // 异常1: ES队列满了(Reject异常)，放回队列
            if (ExceptionUtils.findThrowable(failure, EsRejectedExecutionException.class).isPresent()) {
                indexer.add(action);

                // 异常2: ES超时异常(timeout异常)，放回队列
            } else if (ExceptionUtils.findThrowable(failure, SocketTimeoutException.class).isPresent()) {
                indexer.add(action);

                // 异常3: ES语法异常，丢弃数据，记录日志
            } else if (ExceptionUtils.findThrowable(failure, ElasticsearchParseException.class).isPresent()) {
                log.error("Sink to es parse exception, exceptionData: {} ,exceptionStackTrace: {}",
                        action.toString(), org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(failure));
                // 异常4: 其他异常，丢弃数据，记录日志
                throw failure;
            } else {
                log.error("Sink to es exception, exceptionData: {} ,exceptionStackTrace: {}",
                        action.toString(), org.apache.commons.lang.exception.ExceptionUtils.getFullStackTrace(failure));
                throw failure;
            }
        }
    }
}
