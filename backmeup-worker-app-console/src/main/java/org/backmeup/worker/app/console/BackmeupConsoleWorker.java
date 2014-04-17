package org.backmeup.worker.app.console;

import java.io.IOException;

import org.backmeup.plugin.osgi.PluginImpl;
import org.backmeup.worker.WorkerCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class BackmeupConsoleWorker {
	private static final Logger logger = LoggerFactory.getLogger(BackmeupConsoleWorker.class);

	public static void main(String[] args) {
		logger.info("Starting backmeup worker core");
		
		System.out.println("Starting backmeup worker ... ");
		WorkerCore worker = new WorkerCore();
		worker.start();
		System.out.println("done.");
		
		logger.info("Backmeup worker core startet");
		
		System.out.println("Press any key to quit");
		try {
			System.in.read();
		} catch (IOException e) {
			logger.error("", e);
		}
		
		logger.info("Shutting down backmeup worker core");
		worker.shutdown();
		logger.info("Shutdown complete");
		
		

	}

}
