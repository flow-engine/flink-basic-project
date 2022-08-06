package com._4paradigm.csp.operator.utils;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;

@Slf4j
public class JDBCUtil {

    public static Connection getConnection(String driver, String url, String user, String password) {
        try {
            Class.forName(driver);
            return DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            log.error("JDBC [dirver: {}, url: {}, user: {}, password: {}] Get Connection Error. msg = {}",
                    driver, url, user, password, e.getMessage(), e);
            throw new RuntimeException("Get JDBC Connection error", e);
        }
    }
}
