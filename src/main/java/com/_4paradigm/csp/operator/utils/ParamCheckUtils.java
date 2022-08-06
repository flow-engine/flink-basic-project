package com._4paradigm.csp.operator.utils;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ParamCheckUtils {

    public static String ERROR_MSG_EXPRESSION_TRUE = "%s not valid!";
    public static String ERROR_MSG_STRING_NOT_NULL = "%s is null or empty!";
    public static String ERROR_MSG_OBJECT_NOT_NULL = "%s is null!";
    public static String ERROR_MSG_VALUE_NOT_NULL_NEGATIVE = "%s is null or negative!";

    public static void checkTrue(boolean expression, String arg) {
        if (!expression) {
            throw new RuntimeException(String.format(ERROR_MSG_EXPRESSION_TRUE, arg));
        }
    }

    public static void checkStringNotEmpty(String argValue, String argName) {
        if (argValue == null || argValue.isEmpty()) {
            throw new RuntimeException(String.format(ERROR_MSG_STRING_NOT_NULL, argName));
        }
    }

    public static void checkObjectNotNull(Object item, String arg) {
        if (item == null) {
            throw new RuntimeException(String.format(ERROR_MSG_OBJECT_NOT_NULL, arg));
        }
    }

    public static void checkValueNotNullOrNegative(Integer value, String arg) {
        if (value == null || value < 0) {
            throw new RuntimeException(String.format(ERROR_MSG_VALUE_NOT_NULL_NEGATIVE, arg));
        }
    }

    public static void checkValueNotNullOrNegative(Long value, String arg) {
        if (value == null || value < 0) {
            throw new RuntimeException(String.format(ERROR_MSG_VALUE_NOT_NULL_NEGATIVE, arg));
        }
    }
}
