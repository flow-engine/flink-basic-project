package com._4paradigm.csp.operator.utils;

import com.google.common.base.Strings;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;

/**
 * @see com._4paradigm.simba.contract.PreviewRequest
 */
@Slf4j
public class ASCIIUtil {

    public static String tryToDecode(String mayBeHexString) {
        if (!Strings.isNullOrEmpty(mayBeHexString) && mayBeHexString.startsWith("0x")) {
            return ASCIIUtil.decodeHexString(mayBeHexString);
        }
        return mayBeHexString;
    }

    /**
     * 0x20 to " "
     *
     * @param hexString
     * @return
     */
    public static String decodeHexString(String hexString) {
        byte[] array = new byte[(hexString.length() - 2) / 2];
        for (int i = 0; i < array.length; i++) {
            int start = 2 + 2 * i;
            try {
                array[i] = Byte.decode("0x" + hexString.substring(start, start + 2));
            } catch (NumberFormatException e) {
                log.debug(String.format("ASCII String [%s] error!", hexString), e);
                return null;
            }
        }
        return new String(array, StandardCharsets.US_ASCII);
    }

    /**
     * " " to 0x20
     *
     * @param string
     * @return
     */
    public static String encodeToHexString(String string) {
        byte[] bytes = string.getBytes();
        StringBuilder sb = new StringBuilder(bytes.length * 2 + 2);
        sb.append("0x");
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        return sb.toString();
    }
}
