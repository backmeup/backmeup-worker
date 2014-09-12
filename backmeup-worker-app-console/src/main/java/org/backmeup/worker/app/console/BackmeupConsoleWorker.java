package org.backmeup.worker.app.console;

import java.io.IOException;

import org.backmeup.worker.WorkerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BackmeupConsoleWorker {
	private static final Logger LOGGER = LoggerFactory.getLogger(BackmeupConsoleWorker.class);
	
	private BackmeupConsoleWorker() {
		
	}

	public static void main(String[] args) {
		LOGGER.info("Starting backmeup worker core");
		WorkerCore worker = new WorkerCore();
		
		LOGGER.info("Initializing worker");
		worker.initialize();
		LOGGER.info("Initializing worker done.");
		
		LOGGER.info("Starting worker");
		worker.start();
		LOGGER.info("Starting worker done.");
		
		LOGGER.info("Backmeup worker core startet");
		LOGGER.info("Press any key to quit");
		try {
			System.in.read();
		} catch (IOException e) {
			LOGGER.error("", e);
		}
		
		LOGGER.info("Shutting down backmeup worker core");
		worker.shutdown();
		LOGGER.info("Shutdown complete");
	}
}
