package com._4paradigm.csp.operator.sink;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * TODO: 缺异常处理
 */
@Slf4j
public class Elasticsearch6SinkFunction implements ElasticsearchSinkFunction<String> {

    private String index;
    private String docType;
    private String uniqueColumn;
    private String versionColumn = PropertiesConstants.SINK_ES_VERSION_COLUMN_DEFAULT;
    private Long versionValue;

    public Elasticsearch6SinkFunction(String index, String docType, String uniqueColumn, String versionColumn, Long versionValue) {
        this.index = Preconditions.checkNotNull(index);
        this.docType = Preconditions.checkNotNull(docType);
        this.uniqueColumn = Preconditions.checkNotNull(uniqueColumn);
        if (versionColumn != null && !versionColumn.isEmpty()) {
            this.versionColumn = versionColumn;
            this.versionValue = Preconditions.checkNotNull(versionValue);
        }
        log.info("Index [{}], DocType [{}], UniqueColumn [{}], VersionColumn [{}], Version [{}]", index, docType, uniqueColumn, versionColumn, versionValue);
    }

    @Override
    public void process(String jsonContent, RuntimeContext ctx, RequestIndexer indexer) {
        JSONObject jsonObject = JSONObject.parseObject(jsonContent);
        Object idObject = jsonObject.get(this.uniqueColumn);
        if (idObject == null) {
            log.error("Skip data that does not contain primary key columns.\nData:[{}], columnName: [{}]", jsonContent, this.uniqueColumn);
            return;
        }
        String id = String.valueOf(idObject);
        indexer.add(createIndexRequest(id, jsonObject));
    }

    /**
     * 此方法会根据 id 全量覆盖 document
     *
     * @param id
     * @param element
     * @return
     */
    public IndexRequest createIndexRequest(String id, JSONObject element) {
        log.debug("Debug value....{}: {}", id, element);
        if (versionValue != null) {
            element.put(versionColumn, versionValue);
        }
        return Requests.indexRequest()
                .index(index)
                .type(docType)
                .id(id)
                .source(JSONObject.toJSONString(element).getBytes(), XContentType.JSON);
    }
}
