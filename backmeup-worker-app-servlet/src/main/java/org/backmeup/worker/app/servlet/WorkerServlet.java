package org.backmeup.worker.app.servlet;

import java.io.IOException;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.backmeup.worker.WorkerCore;
import org.backmeup.worker.WorkerInitialisationTask;
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
        this.worker = new WorkerCore();
    }

    // Servlet initialisation -----------------------------------------
    @Override
    public void init() {
        initWorkerAfterServletInitialisation();
    }

    // Servlet initialisation thread due to-----------------------------------------
    // @see http://themis-buildsrv01.backmeup.at/redmine/issues/232
    public void initWorkerAfterServletInitialisation() {
        final ScheduledExecutorService exec = Executors.newScheduledThreadPool(1);
        WorkerInitialisationTask task = new WorkerInitialisationTask(this.worker);
        int SECONDS_BEFORE_INITIALISATION = 15;
        exec.schedule(task, SECONDS_BEFORE_INITIALISATION, TimeUnit.SECONDS);
        LOGGER.info("scheduled worker initialisation in " + SECONDS_BEFORE_INITIALISATION + " seconds.");
    }

    @Override
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws IOException {
        WorkerData workerData = new WorkerData();
        workerData.setWorkerState(this.worker.getCurrentState());
        workerData.setNoOfCurrentJobs(this.worker.getNoOfCurrentJobs());
        workerData.setNoOfMaximumJobs(this.worker.getNoOfMaximumJobs());
        workerData.setNoOfFetchedJobs(this.worker.getNoOfFetchedJobs());
        workerData.setNoOfFinishedJobs(this.worker.getNoOfFinishedJobs());
        workerData.setNoOfFailedJobs(this.worker.getNoOfFailedJobs());

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
        this.worker.shutdown();
        LOGGER.info("Shutdown complete");
    }
}
