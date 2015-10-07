package org.backmeup.worker.utils;

public class StringUtils {
    private StringUtils() {
        // Utility classes should not have public constructor
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
}
