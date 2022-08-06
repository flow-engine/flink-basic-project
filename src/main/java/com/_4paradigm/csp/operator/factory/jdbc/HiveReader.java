package com._4paradigm.csp.operator.factory.jdbc;

import com._4paradigm.csp.operator.utils.JDBCUtil;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * TODO: 需要测试下分区表
 */
@Slf4j
public class HiveReader extends AbstractJDBCReader {

    private static final String ESCAPER = "`";

    @Override
    public RowTypeInfo getTableRowTypeInfo(String driver, String url, String user, String password, String table) throws SQLException {
        Connection connection = JDBCUtil.getConnection(driver, url, user, password);
        try {
            // column and rowType
            TableSchema tableSchema = getHiveColumnsInfo(connection, table);
            return new RowTypeInfo(tableSchema.getFieldTypes(), tableSchema.getFieldNames());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    public static TableSchema getHiveColumnsInfo(Connection conn, String table) {
        TableSchema.Builder builder = new TableSchema.Builder();
        String describeTable = "describe formatted " + ESCAPER + table + ESCAPER;
        // auto close ResultSet
        try (ResultSet resultSet = conn.prepareStatement(describeTable).executeQuery()) {
            int stop = 0;
            while (resultSet.next() && stop < 2) {
                String colName = resultSet.getString(1);
                String colType = resultSet.getString(2);
                log.info("col_name: [{}], data_type: [{}]", colName, colType);
                if (colName.contains("#")) {
                    // 带有 # 开头的行表示是某一种元数据开始处理了，我们只获取原始建表的列的元数据
                    // 所以碰到第二个 # 时，就可以跳出循环
                    stop++;
                    continue;
                }
                if (Strings.isNullOrEmpty(colName)) {
                    continue;
                }
                builder.field(colName, HiveReader.hiveType2DataType(colType));
            }
            return builder.build();
        } catch (SQLException e) {
            throw new RuntimeException(String.format("Execute get jdbc table schema error: %s", e.getMessage()), e);
        }
    }

    public static DataType hiveType2DataType(String hiveType) {
        String upperCaseType = hiveType.toUpperCase();
        if (upperCaseType.startsWith("CHAR") || upperCaseType.startsWith("VARCHAR")) {
            return DataTypes.STRING();
        }
        if (upperCaseType.startsWith("DECIMAL")) {
            // decimal(10,0)
            return DataTypes.DOUBLE();
        }
        switch (upperCaseType) {
            case "TINYINT":
            case "SMALLINT":
                return DataTypes.SMALLINT();
            case "INT":
                return DataTypes.INT();
            case "BIGINT":
                return DataTypes.BIGINT();
            case "BOOLEAN":
                return DataTypes.BOOLEAN();
            case "FLOAT":
                return DataTypes.FLOAT();
            case "DOUBLE":
            case "DECIMAL":
                return DataTypes.DOUBLE();
            case "TIMESTAMP":
                return DataTypes.TIMESTAMP();
            case "DATE":
                return DataTypes.DATE();
            default:
                // STRING,BINARY; ARRAY,MAP,STRUCT,UNION
                return DataTypes.STRING();
        }
    }
}
