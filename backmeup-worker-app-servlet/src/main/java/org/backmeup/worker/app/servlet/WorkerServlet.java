package org.backmeup.worker.app.servlet;

import java.io.IOException;
import java.io.PrintWriter;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.backmeup.worker.WorkerCore;
import org.backmeup.worker.app.servlet.model.WorkerData;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkerServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;
	private static final Logger logger = LoggerFactory.getLogger(WorkerServlet.class);
	
	private final WorkerCore worker;

    public WorkerServlet() {
    	logger.info("Starting backmeup worker core");
    	worker = new WorkerCore();
    }
    
    public void init() throws ServletException {
    	logger.info("Initializing worker");
		worker.initialize();
		logger.info("Initializing worker done.");
		
		logger.info("Starting worker");
		worker.start();
		logger.info("Starting worker done.");
    }
    
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		WorkerData workerData = new WorkerData();
		workerData.setWorkerState(worker.getCurrentState());
		workerData.setNoOfCurrentJobs(worker.getNoOfCurrentJobs());
		workerData.setNoOfMaximumJobs(worker.getNoOfMaximumJobs());
		workerData.setNoOfFetchedJobs(worker.getNoOfFetchedJobs());
		workerData.setNoOfFinishedJobs(worker.getNoOfFinishedJobs());
		workerData.setNoOfFailedJobs(worker.getNoOfFailedJobs());
		
		
		response.setContentType("application/json");
		
		// Disable caching
		// Set standard HTTP/1.1 no-cache headers.
		response.setHeader("Cache-Control", "private, no-store, no-cache, must-revalidate");

		// Set standard HTTP/1.0 no-cache header.
		response.setHeader("Pragma", "no-cache");
		
		
		ObjectMapper mapper = new ObjectMapper();
		PrintWriter out = response.getWriter();
        mapper.writeValue(out, workerData);
	}
	
	public void destroy() {
		logger.info("Shutting down backmeup worker core");
		worker.shutdown();
		logger.info("Shutdown complete");
    }
}
