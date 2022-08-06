package com._4paradigm.csp.operator.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * https://stackoverflow.com/questions/37476898/read-last-commit-of-the-git-and-commit-number
 */
@Slf4j
public class GitUtil {

    public static void printGitInfo() {
        Properties prop = new Properties();
        try {
            InputStream inputStream = GitUtil.class.getClassLoader()
                    .getResourceAsStream("git.properties");
            if (inputStream == null) {
                return;
            }
            prop.load(inputStream);
            inputStream.close();
            String commitId = prop.getProperty("git.commit.id");
            String buildTime = prop.getProperty("git.build.time");
            String buildVersion = prop.getProperty("git.build.version");
            String branchName = prop.getProperty("git.branch");
            log.info("Current branch: {}, build version: {}", branchName, buildVersion);
            log.info("Current CommitId: {}", commitId);
            log.info("Build Time: {}", buildTime);
        } catch (IOException e) {
            log.error("Loading git.properties file faild", e);
        }
    }
}
