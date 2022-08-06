package com._4paradigm.csp.operator.utils;

import org.apache.commons.codec.digest.DigestUtils;

public class MD5Util {

    /**
     * 32 个字节
     * a -> 0CC175B9C0F1B6A831C399E269772661
     *
     * @param content
     * @return
     */
    public static String md5HexUpperCase(String content) {
        return DigestUtils.md5Hex(content).toUpperCase();
    }
}
