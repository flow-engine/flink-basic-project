package com._4paradigm.csp.operator.factory.convert;

import com._4paradigm.csp.operator.utils.ESSinkUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * 根据 "id" 查询之前是否存在此记录
 */
@Slf4j
public class EsItemExistConvert extends RichFlatMapFunction<String, Tuple2<Boolean, String>> {

    private String hosts;
    private String index;
    private String docType;
    private String uniqueColumn;
    private transient RestHighLevelClient client;

    public EsItemExistConvert(String hosts, String index, String docType, String uniqueColumn) {
        this.hosts = hosts;
        this.index = index;
        this.docType = docType;
        this.uniqueColumn = uniqueColumn;
    }

    @Override
    public void open(org.apache.flink.configuration.Configuration parameters) throws Exception {
        super.open(parameters);
        client = ESSinkUtil.createClient(hosts);
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (client != null) {
            client.close();
        }
    }

    /**
     * @param jsonString
     * @param out
     */
    @Override
    public void flatMap(String jsonString, Collector<Tuple2<Boolean, String>> out) {
        JSONObject jsonObject;
        boolean exists;
        try {
            jsonObject = JSONObject.parseObject(jsonString);
            Object uniqueObj = jsonObject.get(uniqueColumn);
            if (uniqueObj == null) {
                log.error("Skip data that does not contain primary key columns.\nData:[{}], columnName: [{}]", jsonString, this.uniqueColumn);
                return;
            }
            String id = String.valueOf(uniqueObj);
            exists = true;
//            exists = client.exists(Requests.getRequest(index).type(docType).id(id), RequestOptions.DEFAULT);
            log.debug("ID [{}] exists [{}]", id, exists);
        } catch (Exception e) {
            log.error("Parse Error", e);
            return;
        }
        out.collect(Tuple2.of(exists, jsonString));
    }
}
