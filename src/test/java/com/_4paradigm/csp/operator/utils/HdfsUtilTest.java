package com._4paradigm.csp.operator.utils;

import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.orc.TypeDescription;
import org.apache.parquet.schema.MessageType;
import org.junit.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

public class HdfsUtilTest {
    private HdfsUtil hdfsUtil = new HdfsUtil("hadoop/cdh", "work");

    @Test
    public void testListFiles() {
        List<LocatedFileStatus> files = hdfsUtil.listFiles("hdfs:///user/liyihui", null);
        assertThat(files.size()).isEqualTo(1L);
    }

    @Test
    public void testReadTextFirstLine() {
        String firstLine = hdfsUtil.readFirstLine("hdfs:///user/liyihui/csv", "\n");
        System.out.println(firstLine);
        assertThat(firstLine).isNotEmpty();
    }

    @Test
    public void testReadOrcSchema() {
        TypeDescription typeDescription = hdfsUtil.readOrcSchema("hdfs:///user/liyihui/orc");
        System.out.println(typeDescription.toJson());
        assertThat(typeDescription).isNotNull();
    }

    @Test
    public void testReadParquetSchema() {
        MessageType messageType = hdfsUtil.readParquetSchema("hdfs:///user/liyihui/parquet");
        System.out.println(messageType.getColumns());
        assertThat(messageType).isNotNull();
    }
}
