package org.backmeup.worker;

public class WorkerException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public WorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public WorkerException(String message) {
        super(message);
    }
    
    public WorkerException(Throwable cause) {
        super(cause);
    }

}
