package org.backmeup.worker.utils;

import java.nio.ByteBuffer;

public final class ByteUtils {
    private ByteUtils() {
        // Utility classes should not have public constructor
    }
    
    public static long bytesToLong(byte[] bytes) {
        ByteBuffer buffer = ByteBuffer.allocate(Long.SIZE);
        buffer.put(bytes);
        buffer.flip();
        return buffer.getLong();
    }
}
