package com._4paradigm.csp.operator;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.DataSourceFactory;
import com._4paradigm.csp.operator.utils.CheckPointUtil;
import com._4paradigm.csp.operator.utils.ExecutionEnvUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.types.Row;
import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;

@Slf4j
public class Elasticsearch6SinkExample {

    public static void main(String[] args) throws Exception {

        final ParameterTool parameterTool = ExecutionEnvUtil.createParameterTool(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        CheckPointUtil.setCheckpointConfig(env, parameterTool);
        DataStream dataStreamSource = DataSourceFactory.readDataSource(env, parameterTool);

        DataStream esSource = dataStreamSource.map((MapFunction<Row, Tuple2<String, String>>) row -> {
            String[] test = row.toString().split(",");
            return new Tuple2(test[0], test[1]);
        }).returns(new TypeHint<Tuple2<String, String>>() {});

        List<HttpHost> httpHosts = new ArrayList<>();
        String esHost = parameterTool.get(PropertiesConstants.SINK_ES_HOST);
        httpHosts.add(new HttpHost(esHost, 9200, "http"));

        ElasticsearchSink.Builder<Tuple2<String, String>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Tuple2<String,String>> (){
                    @Override
                    public void process(Tuple2<String, String> stringStringTuple2, RuntimeContext runtimeContext,
                            RequestIndexer requestIndexer) {
                        requestIndexer.add(createIndexRequest(stringStringTuple2));
                    }
                    public IndexRequest createIndexRequest(Tuple2<String, String> element) {
                        Map<String, String> json = new HashMap<>();
                        //将需要写入ES的字段依次添加到Map当中
                        json.put("data", element.f0);

                        return Requests.indexRequest()
                                .index("my-index")
                                .type("my-type")
                                .source(json);
                    }
                });

        // this instructs the sink to emit after every element, otherwise they would be buffered
        esSinkBuilder.setBulkFlushMaxActions(1);

        esSource.addSink(esSinkBuilder.build());

        env.execute("Elasticsearch 6.x end to end sink test example");
    }

    private static class CustomFailureHandler implements ActionRequestFailureHandler {

        private static final long serialVersionUID = 942269087742453482L;

        private final String index;
        private final String type;

        CustomFailureHandler(String index, String type) {
            this.index = index;
            this.type = type;
        }

        @Override
        public void onFailure(ActionRequest action, Throwable failure, int restStatusCode, RequestIndexer indexer) throws Throwable {
            if (action instanceof IndexRequest) {
                Map<String, Object> json = new HashMap<>();
                json.put("data", ((IndexRequest) action).source());

                indexer.add(
                        Requests.indexRequest()
                                .index(index)
                                .type(type)
                                .id(((IndexRequest) action).id())
                                .source(json));
            } else {
                throw new IllegalStateException("unexpected");
            }
        }
    }

    private static IndexRequest createIndexRequest(String element, ParameterTool parameterTool) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element);

        String index;
        String type;

        if (element.startsWith("message #15")) {
            index = ":intentional invalid index:";
            type = ":intentional invalid type:";
        } else {
            index = parameterTool.getRequired("index");
            type = parameterTool.getRequired("type");
        }

        return Requests.indexRequest()
                .index(index)
                .type(type)
                .id(element)
                .source(json);
    }

    private static UpdateRequest createUpdateRequest(Tuple2<String, String> element, ParameterTool parameterTool) {
        Map<String, Object> json = new HashMap<>();
        json.put("data", element.f1);

        return new UpdateRequest(
                parameterTool.getRequired("index"),
                parameterTool.getRequired("type"),
                element.f0)
                .doc(json)
                .upsert(json);
    }
}
