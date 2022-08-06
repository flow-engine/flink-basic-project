package com._4paradigm.csp.operator.enums;

public enum SourceType {
    /**
     * jdbc
     */
    jdbc,
    /**
     * fs
     */
    parquet, orc, csv, tsv, txt,
    /**
     * kafka:
     * * kafka(默认 kafka sql)
     * * kafka_deduplication(kafka 去重算子)
     */
    kafka,
    kafka_deduplication
}