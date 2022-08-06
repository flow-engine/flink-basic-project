# 版本问题

`因为目前用的先知的 Flink, 所以分支暂且随先知, 其实并没有强关联关系`

# Release Note

| 分支 | 功能 | 说明 | 
|---|---|---|
| release/3.8.2 | [3.8.2 功能](#release/3.8.2) | 并没有厂部署, 马上就出了 release/3.8.3
| release/3.8.3 | [3.8.3 功能](#release/3.8.3) | 

## Details

### release/3.8.2

[需求](https://wiki.4paradigm.com/pages/viewpage.action?pageId=74767383)

`支持之前的 SDP Table 所有数据源的批量引入对应的流式引入`

* HDFS folder -> Kafka (Sql/自定义 Merge)
* Hive -> Kafka (批处理)
* Kafka -> Kafka SQL
* Kafka -> Kafka 去重

### release/3.8.3

[需求](https://wiki.4paradigm.com/pages/viewpage.action?pageId=76333161#id-%E5%8F%AC%E5%9B%9E%E6%A8%A1%E5%9D%97%E5%AE%9E%E7%8E%B0%E6%96%B9%E6%A1%88-%E5%8F%AC%E5%9B%9E%E6%A8%A1%E5%9D%97%E5%AE%9E%E7%8E%B0%E6%96%B9%E6%A1%88-4sdp-es%E6%95%B0%E6%8D%AE%E5%90%8C%E6%AD%A5%E6%A8%A1%E5%9D%97)

`支持 HDFS Parquet 写入 ES, 主要用于召回模块`

* HDFS Parquet folder -> Elasticsearch 6.x (支持 SQL/自定义函数`硬编码`/自动塞入版本字段)