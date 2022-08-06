package com._4paradigm.csp.operator.utils;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.streaming.connectors.elasticsearch6.RestClientFactory;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ESSinkUtil {
    /**
     * es sink
     *
     * @param hosts               es hosts
     * @param bulkFlushMaxActions bulk flush size
     * @param parallelism         并行数
     * @param data                数据
     * @param func
     * @param <T>
     */
    public static <T> void addSink(List<HttpHost> hosts, int bulkFlushMaxActions, int parallelism,
                                   SingleOutputStreamOperator<T> data, ElasticsearchSinkFunction<T> func,
                                   ParameterTool parameterTool) {
        ElasticsearchSink.Builder<T> esSinkBuilder = new ElasticsearchSink.Builder<>(hosts, func);
        esSinkBuilder.setBulkFlushMaxActions(bulkFlushMaxActions);
//        esSinkBuilder.setFailureHandler(new RetryRequestFailureHandler());
        //todo:xpack security
        data.addSink(esSinkBuilder.build()).setParallelism(parallelism);
    }


    /**
     * 解析配置文件的 es hosts
     *
     * @param hosts
     * @return
     * @throws MalformedURLException
     */
    public static List<HttpHost> getEsAddresses(String hosts) throws MalformedURLException {
        log.info("ES Server: [{}]", hosts);
        String[] hostList = hosts.split(",");
        List<HttpHost> addresses = new ArrayList<>();
        for (String host : hostList) {
            if (host.startsWith("http")) {
                URL url = new URL(host);
                addresses.add(new HttpHost(url.getHost(), url.getPort()));
            } else {
                String[] parts = host.split(":", 2);
                if (parts.length > 1) {
                    addresses.add(new HttpHost(parts[0], Integer.parseInt(parts[1])));
                } else {
                    throw new MalformedURLException("Invalid elasticsearch hosts format");
                }
            }
        }
        return addresses;
    }

    public static RestHighLevelClient createClient(String hosts) throws Exception {
        List<HttpHost> httpHosts = getEsAddresses(hosts);
        RestClientBuilder builder = RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
        RestClientFactory restClientFactory = restClientBuilder -> {
        };
        restClientFactory.configureRestClientBuilder(builder);
        return new RestHighLevelClient(builder);
    }
}
