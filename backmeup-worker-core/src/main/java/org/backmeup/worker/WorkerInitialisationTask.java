package org.backmeup.worker;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerInitialisationTask implements Runnable {

    private final Logger log = LoggerFactory.getLogger(getClass());
    private WorkerCore worker;

    public WorkerInitialisationTask(WorkerCore worker) {
        this.worker = worker;
    }

    @Override
    public void run() {
        this.log.info("Initializing worker");
        this.worker.initialize();
        this.log.info("Initializing worker done.");

        this.log.info("Starting worker");
        this.worker.start();
        this.log.info("Starting worker done.");
    }

}