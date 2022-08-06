package com._4paradigm.csp.operator.factory.jdbc;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

@Slf4j
public abstract class AbstractJDBCReader {

    public abstract RowTypeInfo getTableRowTypeInfo(String driver, String url, String user, String password, String table) throws Exception;

    public JDBCInputFormat getJDBCInputFormat(String driver, String url, String user, String password, String table, String sql, int fetchSize) {
        log.info("JDBC driver: [{}], ulr: [{}], user: [{}], password: [{}], table: [{}], sql: [{}]",
                driver, url, user, password, table, sql);
        JDBCInputFormat.JDBCInputFormatBuilder builder = JDBCInputFormat.buildJDBCInputFormat()
                .setDrivername(driver)
                .setDBUrl(url)
                .setUsername(user)
                .setPassword(password)
                .setFetchSize(fetchSize);
        try {
            // TODO: fetch the sql result schema
            RowTypeInfo rowTypeInfo = this.getTableRowTypeInfo(driver, url, user, password, table);
            log.info("Table [{}]'s rowTypeInfo", table, rowTypeInfo);
            return builder.setQuery(sql).setRowTypeInfo(rowTypeInfo).finish();
        } catch (Exception e) {
            log.error("Build JDBCInputFormat error", e);
            throw new RuntimeException(e);
        }
    }
}
