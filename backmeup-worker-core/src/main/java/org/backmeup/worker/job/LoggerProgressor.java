package org.backmeup.worker.job;

import org.backmeup.model.dto.BackupJobExecutionDTO;
import org.backmeup.plugin.api.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LoggerProgressor implements Progressable {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoggerProgressor.class);
    
    private final BackupJobExecutionDTO job;
    private final String category;

    public LoggerProgressor(BackupJobExecutionDTO job, String category) {
        this.job = job;
        this.category = category;
    }

    @Override
    public void progress(String message) {
        LOGGER.info("Job {} [{}] {}", this.job.getId(), this.category, message);
    }
}
