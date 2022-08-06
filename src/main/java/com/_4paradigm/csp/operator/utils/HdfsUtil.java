package com._4paradigm.csp.operator.utils;

import com._4paradigm.csp.operator.constant.PropertiesConstants;
import com._4paradigm.csp.operator.factory.convert.parser.TextLineParser;
import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.VersionInfo;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.TypeDescription;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.util.HadoopInputFile;
import org.apache.parquet.schema.MessageType;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.file.Paths;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

@Slf4j
public class HdfsUtil {

    // Hadoop 环境变量
    private static final String HADOOP_USER_NAME = "HADOOP_USER_NAME";
    private static final String HADOOP_CONF_DIR = "HADOOP_CONF_DIR";
    // keytab 目前是固定地址的 /etc/user.keytab
    private static final String HADOOP_KEYTAB_VALUE = "/etc/user.keytab";

    private Configuration configuration;
    private UserGroupInformation ugi;

    public static HdfsUtil buildUtil(ParameterTool parameterTool) {
        log.info("Start Debug hadoop env=================================");
        log.info("System.properties HADOOP_USER_NAME [{}], HADOOP_CONF_DIR [{}]", System.getProperty(HADOOP_USER_NAME), System.getProperty(HADOOP_CONF_DIR));
        log.info("System.env HADOOP_USER_NAME [{}], HADOOP_CONF_DIR [{}]", System.getenv(HADOOP_USER_NAME), System.getenv(HADOOP_CONF_DIR));
        log.info("ParameterTool system env HADOOP_USER_NAME [{}], HADOOP_CONF_DIR [{}]", parameterTool.get(HADOOP_USER_NAME), parameterTool.get(PropertiesConstants.HADOOP_USER_NAME));
        log.info("ParameterTool config env HADOOP_USER_NAME [{}], HADOOP_CONF_DIR [{}]", parameterTool.get(PropertiesConstants.HADOOP_USER_NAME), parameterTool.get((PropertiesConstants.HADOOP_HOME_DIR)));
        log.info("End Debug hadoop env=================================");

        String hadoopHome = StringUtils.isNotEmpty(System.getenv(HADOOP_CONF_DIR)) ? System.getenv(HADOOP_CONF_DIR) : parameterTool.get(PropertiesConstants.HADOOP_HOME_DIR);
        String hadoopUserName = StringUtils.isNotEmpty(System.getenv(HADOOP_USER_NAME)) ? System.getenv(HADOOP_USER_NAME) : parameterTool.get(PropertiesConstants.HADOOP_USER_NAME, "work");
        return new HdfsUtil(hadoopHome, hadoopUserName);
    }

    HdfsUtil(String hadoopHomeDir, String hadoopUserName) {
        log.info("Real environment Hadoop.home.dir [{}], Hadoop.user.name [{}]", hadoopHomeDir, hadoopUserName);
        this.configuration = this.initConfiguration(hadoopHomeDir, hadoopUserName);
        this.ugi = this.initUgi(hadoopUserName);
    }

    private Configuration initConfiguration(String hadoopHomeDir, String hadoopUserName) {
        log.info("HADOOP config Home {}", hadoopHomeDir);
        log.info("HADOOP_USER_NAME {}", hadoopUserName);

        if (hadoopHomeDir != null) {
            System.setProperty("hadoop.home.dir", hadoopHomeDir);
            // 设置krb5.conf
            System.setProperty("java.security.krb5.conf", Paths.get(hadoopHomeDir, "krb5.conf").toString());
        }

        System.setProperty("HADOOP_USER_NAME", hadoopUserName);

        Configuration configuration = new Configuration();
        // 加载hdfs xml配置
        if (hadoopHomeDir != null) {
            configuration.addResource(new Path(Paths.get(hadoopHomeDir, "core-default.xml").toString()));
            configuration.addResource(new Path(Paths.get(hadoopHomeDir, "core-site.xml").toString()));
            configuration.addResource(new Path(Paths.get(hadoopHomeDir, "hdfs-site.xml").toString()));
        }

        configuration.setBoolean("fs.hdfs.impl.disable.cache", true);
        // hdfs connection conf
        configuration.set("dfs.client.socket-timeout", "10000"); // PHTEE-2923 default 60_000 ms
        configuration.set("ipc.client.connect.timeout", "10000"); // default 20000 ms
        configuration.set("ipc.client.connect.max.retries.on.timeouts", "2"); // https://hadoop.apache.org/docs/r2.7.2/hadoop-project-dist/hadoop-common/core-default.xml

        log.info("HADOOP CONFIG:\n{}", configuration);
        UserGroupInformation.setConfiguration(configuration);

        log.info("Hadoop Client Version: {}, SecurityEnabled: {}, fs.defaultFS: {}", VersionInfo.getVersion(),
                isKerberosOn(), configuration.get("fs.defaultFS"));
        return configuration;
    }

    private UserGroupInformation initUgi(String hadoopUserName) {
        try {
            UserGroupInformation ugi = null;
            if (isKerberosOn()) {
                File keytab = getKeytabFile(HADOOP_KEYTAB_VALUE);
                if (keytab != null) {
                    ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(hadoopUserName,
                            keytab.getAbsolutePath());
                }
            } else {
                ugi = UserGroupInformation.createRemoteUser(hadoopUserName);
            }
            return ugi;
        } catch (Exception e) {
            log.error("Hadoop User [{}], Keytab [{}], get ugi error {}", hadoopUserName, HADOOP_KEYTAB_VALUE, e.getMessage(), e);
            throw new RuntimeException(String.format("user %s, keytab %s get ugi error %s", hadoopUserName, HADOOP_KEYTAB_VALUE, e.getMessage(), e));
        }
    }

    private static File getKeytabFile(final String keytab) {
        if (Strings.isNullOrEmpty(keytab)) {
            return null;
        }
        if (isBase64(keytab)) {
            Base64.Decoder decoder = Base64.getDecoder();
            try {
                File file = File.createTempFile("pht-", ".keytab", new File("/tmp"));
                FileUtils.writeByteArrayToFile(file, decoder.decode(keytab));
                return file;
            } catch (IOException e) {
                log.error("create keytab file msg", e);
            }
        } else {
            File file = new File(keytab);
            if (file.exists() && file.canRead()) {
                return file;
            } else {
                log.error("keytab file do not exists or can not readable [{}]", keytab);
            }
        }
        return null;
    }

    private static boolean isBase64(final String string) {
        return org.apache.commons.net.util.Base64.isArrayByteBase64(string.getBytes());
    }

    private static boolean isKerberosOn() {
        return UserGroupInformation.isSecurityEnabled();
    }

    public List<LocatedFileStatus> listFiles(String uri, String ends) {
        final URI standardURI = URI.create(uri);
        return ugi.doAs((PrivilegedAction<List<LocatedFileStatus>>) () -> {
            try (FileSystem fileSystem = newFileSystem(standardURI)) {
                Path path = new Path(standardURI.getPath());
                List<LocatedFileStatus> locatedFileStatuses = new ArrayList<>();
                RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fileSystem
                        .listFiles(path, false);
                while (fileStatusRemoteIterator.hasNext()) {
                    LocatedFileStatus cur = fileStatusRemoteIterator.next();
                    if (cur.isFile()) {
                        String fileName = cur.getPath().getName();
                        if (fileName.startsWith(".") || fileName.startsWith("_")) {
                            continue;
                        }
                        if (ends == null || fileName.endsWith(ends)) {
                            locatedFileStatuses.add(cur);
                        }
                    }
                }
                return locatedFileStatuses;
            } catch (FileNotFoundException e) {
                log.error("list files [{}] FileNotFound error", uri, e);
                throw new RuntimeException("list files FileNotFound error", e);
            } catch (IOException e) {
                log.error("list files [{}] io error", uri, e);
                throw new RuntimeException("list file io error", e);
            } catch (IllegalArgumentException e) {
                log.error("list files [{}] illegalArgument error", uri, e);
                throw new RuntimeException("list files error", e);
            }
        });
    }

    private String getTextFirstLine(String uri, String lineDelimiter) {
        final URI standardURI = URI.create(uri);
        return ugi.doAs((PrivilegedAction<String>) () -> {
            try (FileSystem fileSystem = newFileSystem(standardURI)) {
                Path path = new Path(standardURI);
                try (FSDataInputStream inputStream = fileSystem.open(path)) {
                    try (BufferedReader br = new BufferedReader(
                            new InputStreamReader(inputStream))) {
                        String line;
                        while ((line = br.readLine()) != null) {
                            String firstLine = line.split(TextLineParser.escapeSpecialRegexChars(lineDelimiter))[0].trim();
                            if (firstLine != null) {
                                return firstLine;
                            }
                        }
                    }
                } catch (FileNotFoundException e) {
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    log.error("io stream copy error", e);
                    throw new RuntimeException("io stream copy error!", e);
                }
            } catch (IOException e) {
                throw new RuntimeException("new file system error!", e);
            }
            return null;
        });
    }

    public String readFirstLine(String path, String lineDelimiter) {
        List<LocatedFileStatus> files = listFiles(path, null);

        if (files == null || files.isEmpty()) {
            throw new RuntimeException(String.format("Empty path [%s] ", path));
        }

        String filePath = files.get(0).getPath().toString();
        log.info("Text First File: {}", filePath);
        String firstLine = getTextFirstLine(filePath, lineDelimiter);
        if (firstLine == null) {
            throw new RuntimeException(String.format("Path [%s]'s file [{%s}], first line is null", path, filePath));
        }
        return firstLine;
    }

    public TypeDescription readOrcSchema(String path) {
        List<LocatedFileStatus> files = listFiles(path, null);

        if (files == null || files.isEmpty()) {
            throw new RuntimeException(String.format("Empty path [%s] ", path));
        }
        // TODO: check schema
        Path firstPath = files.get(0).getPath();
        log.info("Orc folder first file: [{}]", firstPath);
        return ugi.doAs((PrivilegedAction<TypeDescription>) () -> {
            try {
                Reader reader = OrcFile.createReader(
                        firstPath, OrcFile.readerOptions(configuration));
                return reader.getSchema();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    public MessageType readParquetSchema(String path) {
        List<LocatedFileStatus> files = listFiles(path, null);
        if (files == null || files.isEmpty()) {
            throw new RuntimeException(String.format("Empty path [%s] ", path));
        }
        // TODO: check schema
        Path firstPath = files.get(0).getPath();
        log.info("Parquet Folder first file: {}", firstPath);
        return ugi.doAs((PrivilegedAction<MessageType>) () -> {
            try (ParquetFileReader r = ParquetFileReader.open(HadoopInputFile.fromPath(firstPath, this.configuration))) {
                final MessageType schema = r.getFooter().getFileMetaData().getSchema();
                return schema;
            } catch (IOException e) {
                log.error("Read Parquet file {} error.", firstPath, e);
                throw new RuntimeException(String.format("Error read Parquet file schema, path [%s].", files.get(0)), e);
            }
        });
    }

    private FileSystem newFileSystem(final URI uri) throws IOException {
        if (uri.getScheme() != null) {
            return FileSystem.newInstance(uri, configuration);
        }
        return FileSystem.newInstance(configuration);
    }

}
