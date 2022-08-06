package com._4paradigm.csp.operator.source;

import com._4paradigm.csp.operator.utils.JDBCUtil;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

@Slf4j
public class JDBCSource extends RichSourceFunction<String> {

    PreparedStatement ps;
    private Connection connection;

    private String driver;
    private String url;
    private String user;
    private String password;
    private String sql;
    private int fetchSize;

    public JDBCSource(String driver, String url, String user, String password, String sql, int fetchSize) {
        log.info("JDBCSource init.... driver [{}], url [{}], user: [{}], password: [{}], sql: [{}], fetchSize: [{}]",
                driver, url, user, password, sql, fetchSize);
        this.driver = driver;
        this.url = url;
        this.user = user;
        this.password = password;
        this.sql = sql;
        this.fetchSize = fetchSize;
    }

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接。
     *
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = JDBCUtil.getConnection(driver,
                url, user, password);
        ps = this.connection.prepareStatement(sql);
        if (fetchSize == Integer.MIN_VALUE || fetchSize > 0) {
            ps.setFetchSize(this.fetchSize);
        }
    }

    /**
     * 程序执行完毕就可以进行，关闭连接和释放资源的动作了
     *
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        super.close();
        //关闭连接和释放资源
        if (ps != null) {
            ps.close();
        }

        if (connection != null) {
            connection.close();
        }
    }

    /**
     * DataStream 调用一次 run() 方法用来获取数据
     *
     * @param ctx
     * @throws Exception
     */
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        ResultSet resultSet = ps.executeQuery();

        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            JSONObject jsonObject = new JSONObject();

            for (int i = 0; i < columnCount; i++) {
                String columnName = metaData.getColumnName(i + 1);
                // select * 返回的列名，cdh fi 为 <table>.<column>， tdh 为 <column>
                String[] res = columnName.split("\\.");
                columnName = res[res.length - 1];

                String value = resultSet.getString(i + 1);
                jsonObject.put(columnName, value);
            }
            ctx.collect(jsonObject.toJSONString());
        }
    }

    @Override
    public void cancel() {
    }
}
