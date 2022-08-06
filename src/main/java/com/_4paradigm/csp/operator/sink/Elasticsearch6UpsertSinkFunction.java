package com._4paradigm.csp.operator.sink;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.enums.SyncEsType;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * TODO: T -> Tuple<Boolean, Row>
 * Boolean 为 ES 中是否存在
 * <p>
 */
@Slf4j
public class Elasticsearch6UpsertSinkFunction implements ElasticsearchSinkFunction<Tuple2<Boolean, String>> {

    private String index;
    private String docType;
    private String uniqueColumn;
    private SyncEsType syncEsType;
    private String versionColumn = PropertiesConstants.SINK_ES_VERSION_COLUMN_DEFAULT;
    private Long versionValue;
    private int retryOnConflict;

    public Elasticsearch6UpsertSinkFunction(String index, String docType, String uniqueColumn, SyncEsType syncEsType,
                                            String versionColumn, Long versionValue, int retryOnConflict) {
        this.index = Preconditions.checkNotNull(index);
        this.docType = Preconditions.checkNotNull(docType);
        this.uniqueColumn = Preconditions.checkNotNull(uniqueColumn);
        this.syncEsType = syncEsType;
        if (SyncEsType.full.equals(syncEsType)) {
            throw new RuntimeException("Elasticsearch6UpsertSinkFunction not support syncType [full], Please use Elasticsearch6SinkFunction");
        }
        if (SyncEsType.incremental.equals(syncEsType)) {
            if (versionColumn != null && !versionColumn.isEmpty()) {
                this.versionColumn = versionColumn;
                this.versionValue = Preconditions.checkNotNull(versionValue);
                log.info("ES incremental sync versionColumn [{}], versionValue [{}]", versionColumn, versionValue);
            }
        }
        this.retryOnConflict = retryOnConflict;
        log.info("Index [{}], DocType [{}], UniqueColumn [{}], SyncType [{}]", index, docType, uniqueColumn, syncEsType);
    }

    /**
     * * incremental: 增量同步: 只需要同步更新(有记录, Merge 更新; 没有记录: 塞入)
     * * finite : 限定同步:只有在es对应记录存在才要更新
     */
    @Override
    public void process(Tuple2<Boolean, String> element, RuntimeContext ctx, RequestIndexer indexer) {
        boolean exists = element.f0;
        JSONObject jsonObject = JSONObject.parseObject(element.f1);
        Object idObj = jsonObject.get(this.uniqueColumn);
        if (idObj == null) {
            log.error("Skip data that does not contain primary key columns.\nData:[{}], columnName: [{}]", element.f1, this.uniqueColumn);
            return;
        }
        String id = String.valueOf(idObj);

        try {
            if (SyncEsType.incremental.equals(this.syncEsType)) {
                processIncremental(exists, id, jsonObject, indexer);
            } else if (SyncEsType.finite.equals(this.syncEsType)) {
                processFinite(exists, id, jsonObject, indexer);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    // finite : 限定同步:只有在es对应记录存在才要更新
    private void processFinite(boolean exists, String id, JSONObject jsonObject, RequestIndexer indexer) throws IOException {
        if (exists) {
            indexer.add(updateIndexRequest(id, jsonObject));
        }
    }

    // incremental: 增量同步: 只需要同步更新(有记录, Merge 更新; 没有记录: 塞入)
    private void processIncremental(Boolean exists, String id, JSONObject element, RequestIndexer indexer) throws Exception {
        log.debug("Debug value....{}: {}", id, element);
        if (versionValue != null) {
            if (element.containsKey(versionColumn)) {
                log.error("Skip the data that contains the version field.\nData [{}], versionColumn [{}]", element, versionColumn);
                return;
            }
            element.put(versionColumn, versionValue);
        }
        if (exists) {
            // upsert
            indexer.add(updateIndexRequest(id, element));
        } else {
            indexer.add(createIndexRequest(id, element));
        }
    }

    /**
     * 此方法会根据 id 全量覆盖 document
     *
     * @param id
     * @param element
     * @return
     */
    public IndexRequest createIndexRequest(String id, JSONObject element) {
        return Requests.indexRequest()
                .index(index)
                .type(docType)
                .id(id)
                .source(JSONObject.toJSONString(element).getBytes(), XContentType.JSON);
    }

    public UpdateRequest updateIndexRequest(String id, JSONObject element) throws IOException {
        UpdateRequest updateRequest = new UpdateRequest();
        //设置表的index和type, 必须设置id才能update
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder().startObject();
        for (Map.Entry<String, Object> entry : element.entrySet()) {
            if (entry.getValue() != null) {
                xContentBuilder.field(entry.getKey(), entry.getValue());
            }
        }
        XContentBuilder data = xContentBuilder.endObject();
        updateRequest.index(index).type(docType).id(id)
                .doc(data);
        updateRequest.retryOnConflict(retryOnConflict);
        return updateRequest;
    }

}
