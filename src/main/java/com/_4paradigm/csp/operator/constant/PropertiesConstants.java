package com._4paradigm.csp.operator.constant;


public class PropertiesConstants {
    public static final String JOB_NAME = "job.name";
    public static final String PROPERTIES_FILE_NAME = "/application.properties";
    // source (fs info)
    public static final String SOURCE_TYPE = "source.type";
    public static final String SOURCE_PATH = "source.path";
    public static final String SOURCE_WATCH_TYPE = "source.watch.type";
    // 单位 ms
    public static final String SOURCE_WATCH_INTERVAL = "source.watch.interval";
    // # source txt/csv other info
    public static final String SOURCE_FIRST_LINE_SCHEMA = "source.firstLine.schema";
    public static final String SOURCE_LINE_DELIMITER = "source.line.delimiter";
    public static final String SOURCE_FIELD_DELIMITER = "source.field.delimiter";
    public static final String SOURCE_CHARSET = "source.charset";

    // source (db info)
    public static final String SOURCE_DB_TYPE = "source.db.type";
    public static final String SOURCE_DB_URL = "source.db.url";
    public static final String SOURCE_DB_USERNAME = "source.db.username";
    public static final String SOURCE_DB_PASSWORD = "source.db.password";
    public static final String SOURCE_DB_DRIVER = "source.db.driverClassName";
    public static final String SOURCE_DB_DRIVER_DEFAULT = "org.apache.hive.jdbc.HiveDriver";
    public static final String SOURCE_DB_TABLE = "source.db.table";
    public static final String SOURCE_DB_SQL = "source.db.sql";
    public static final String SOURCE_DB_FETCH_SIZE = "source.db.fetch.size";
    public static final int SOURCE_DB_FETCH_SIZE_DEFAULT = 50;

    // source (kafka info)
    public static final String SOURCE_KAFKA_VERSION = "source.kafka.version";
    public static final String SOURCE_KAFKA_BROKERS = "source.kafka.brokers";
    public static final String SOURCE_KAFKA_TOPIC = "source.kafka.topic";
    public static final String CONSUMER_KAFKA_AUTO_OFFSET_RESET = "auto.offset.reset";
    public static final String CONSUMER_GROUP_ID = "group.id";
    // kafka -> kafka sql 相关
    // 流式引入是否开启 sql, default false
    public static final String STREAM_SQL_ENABLE = "stream.sql.enable";
    public static final String STREAM_SQL_TABLE = "stream.sql.table";
    public static final String STREAM_SQL = "stream.sql";
    public static final String STREAM_SQL_SCHEMA = "stream.sql.schema";
    public static final String STREAM_SQL_SCHEMA_JSON = "stream.sql.schema.json";

    // sink (kafka info)
    public static final String SINK_TYPE = "sink.type";
    public static final String SINK_KAFKA_VERSION = "sink.kafka.version";
    public static final String SINK_KAFKA_BROKERS = "sink.kafka.brokers";
    public static final String SINK_KAFKA_TOPIC = "sink.kafka.topic";

    // flink config
    public static final String STREAM_PARALLELISM = "stream.parallelism";
    public static final String STREAM_SINK_PARALLELISM = "stream.sink.parallelism";
    public static final int STREAM_SINK_PARALLELISM_DEFAULT = 1;
    public static final String STREAM_CHECKPOINT_ENABLE = "stream.checkpoint.enable";
    public static final String STREAM_CHECKPOINT_DIR = "stream.checkpoint.dir";
    public static final String STREAM_CHECKPOINT_TYPE = "stream.checkpoint.type";
    public static final String STREAM_CHECKPOINT_INTERVAL = "stream.checkpoint.interval";
    public static final String STREAM_CHECKPOINT_TIMEOUT = "stream.checkpoint.timeout";
    public static final String STREAM_CHECKPOINT_TOLERABLE_FAILURE_NUM = "stream.checkpoint.tolerable.failure.num";
    public static final String STREAM_CHECKPOINT_MIN_PAUSE = "stream.checkpoint.min.pause";
    public static final String STREAM_CHECKPOINT_MODE = "stream.checkpoint.mode";
    // 任务失败重试机制
    public static final String JOB_RETRY_MAX_NUMBER_RESTART_ATTEMPTS = "job.retry.maxNumberRestartAttempts"; // default 4
    public static final String JOB_RETRY_BACK_OFF_TIMEMS = "job.retry.backoffTimeMS"; // 60000

    // 去重配置
    public static final String UNIQUE_DATA_TTL_IN_MINUTE = "unique.data.ttl.in.minute";
    public static final String UNIQUE_KEYS = "unique.keys";

    // hadoop config
    public static final String HADOOP_USER_NAME = "hadoop.user.name";
    public static final String HADOOP_HOME_DIR = "hadoop.home.dir";

    // redis sink
    public static final String SINK_REDIS_HOST = "sink.redis.host";

    // rtidb sink
    public static final String SINK_RTIDB_ADDRESS = "sink.rtidb.address";
    public static final String SINK_RTIDB_PATH = "sink.rtidb.path";
    public static final String SINK_RTIDB_TABLE = "sink.rtidb.table";

    // sds connector
    public static final String SOURCE_CONNECTOR = "sourceConnector";
    public static final String SINK_CONNECTOR = "sinkConnector";

    // sink (db info)
    public static final String SINK_DB_URL = "sink.db.url";
    public static final String SINK_DB_USERNAME = "sink.db.username";
    public static final String SINK_DB_PASSWORD = "sink.db.password";
    public static final String SINK_DB_DRIVER = "sink.db.driverClassName";
    public static final String SINK_DB_TABLE = "sink.db.table";
    public static final String SINK_DB_SQL = "sink.db.sql";



    // merge 输出的数据 (默认 false)
    public static final String SINK_MERGE_ENABLE = "sink.merge.enable";
    // 除了 id, merge 的字段名默认 feat
    public static final String SINK_MERGE_FIELD = "sink.merge.nested.field";
    // 排除 merge 的字段(默认 id)
    public static final String SINK_MERGE_EXCLUDE_FIELDS = "sink.merge.exclude.fields";

    // ES 配置
    // eg. host1:9200,host2:9200
    public static final String SINK_ES_HOST = "sink.es.server";
    public static final String SINK_ES_INDEX = "sink.es.index";
    public static final String SINK_ES_TYPE = "sink.es.type";
    public static final String SINK_ES_SYNC_TYPE = "sink.es.sync.type";
    public static final String SINK_ES_UNIQUE_COLUMN = "sink.es.unique.column";
    // full 同步模式需要, 默认 _tag
    public static final String SINK_ES_VERSION_COLUMN = "sink.es.version.column";
    public static final String SINK_ES_VERSION_COLUMN_DEFAULT = "version";
    public static final String SINK_ES_VERSION = "sink.es.version";
    public static final String SINK_ES_UPDATE_RETRY_ON_CONFLICT = "sink.es.update.retry.on.conflict";

}