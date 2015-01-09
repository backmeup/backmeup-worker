package org.backmeup.worker.config;

import java.io.IOException;
import java.util.Properties;

public final class Configuration {
    private static final Properties PROPERTIES = new Properties();

    private static final String PROPERTYFILE = "backmeup-worker.properties";

    static {
        try {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            if (loader.getResourceAsStream(PROPERTYFILE) != null) {
                PROPERTIES.load(loader.getResourceAsStream(PROPERTYFILE));
            } else {
                throw new IOException("unable to load properties file: " + PROPERTYFILE);
            }
        } catch (IOException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    public static String getProperty(String key) {
        return PROPERTIES.getProperty(key);
    }

    public static String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        if (value == null) {
            return defaultValue;
        }
        return value;
    }
}
