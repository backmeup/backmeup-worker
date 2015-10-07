package org.backmeup.worker.utils;

public class StringUtils {
    private StringUtils() {
        // Utility classes should not have public constructor
    }

    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }
    
    public static String getLastSplitElement(String text, String regex) {
        String[] parts = text.split(regex);

        if (parts.length > 0) {
            return parts[parts.length - 1];
        } else {
            return text;
        }
    }
}
