package com._4paradigm.csp.operator.sink;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com.alibaba.fastjson.JSONObject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

/**
 * TODO: 缺异常处理
 */
@Slf4j
public class Elasticsearch6Sink2Function implements ElasticsearchSinkFunction<Row>, Serializable {

    private String index;
    private String docType;

    public Elasticsearch6Sink2Function(String index, String docType) {
        this.index = Preconditions.checkNotNull(index);
        this.docType = Preconditions.checkNotNull(docType);
    }


    @Override
    public void process(Row row, RuntimeContext runtimeContext, RequestIndexer requestIndexer) {
        requestIndexer.add(createIndexRequest(row));
    }

    public IndexRequest createIndexRequest(Row row) {
        Map<String, String> json = new HashMap<>();
        //将需要写入ES的字段依次添加到Map当中
        json.put("data", row.getField(0).toString());

        return Requests.indexRequest()
                .index(this.index)
                .type(this.docType)
                .source(json);
    }


}
