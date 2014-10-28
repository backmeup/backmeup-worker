package org.backmeup.worker.app.servlet;

import java.io.IOException;
import java.io.PrintWriter;

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
    private static final Logger LOGGER = LoggerFactory.getLogger(WorkerServlet.class);

    private final WorkerCore worker;

    public WorkerServlet() {
        LOGGER.info("Starting backmeup worker core");
        worker = new WorkerCore();
    }

    @Override
    public void init() {
        LOGGER.info("Initializing worker");
        worker.initialize();
        LOGGER.info("Initializing worker done.");

        LOGGER.info("Starting worker");
        worker.start();
        LOGGER.info("Starting worker done.");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
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
        try (PrintWriter out = response.getWriter()) {
            mapper.writeValue(out, workerData);
        }
    }

    @Override
    public void destroy() {
        LOGGER.info("Shutting down backmeup worker core");
        worker.shutdown();
        LOGGER.info("Shutdown complete");
    }
}
