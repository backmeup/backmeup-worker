package org.backmeup.worker.app.servlet.model;

import org.backmeup.worker.WorkerCore.WorkerState;

public class WorkerData {
	private WorkerState workerState;
	private int NoOfCurrentJobs;
	private int NoOfMaximumJobs;
	private int noOfFetchedJobs;
	private int noOfFinishedJobs;
	private int noOfFailedJobs;
	
	public WorkerData() {
		
	}

	public WorkerState getWorkerState() {
		return workerState;
	}

	public void setWorkerState(WorkerState workerState) {
		this.workerState = workerState;
	}

	public int getNoOfCurrentJobs() {
		return NoOfCurrentJobs;
	}

	public void setNoOfCurrentJobs(int noOfCurrentJobs) {
		NoOfCurrentJobs = noOfCurrentJobs;
	}

	public int getNoOfMaximumJobs() {
		return NoOfMaximumJobs;
	}

	public void setNoOfMaximumJobs(int noOfMaximumJobs) {
		NoOfMaximumJobs = noOfMaximumJobs;
	}

	public int getNoOfFetchedJobs() {
		return noOfFetchedJobs;
	}

	public void setNoOfFetchedJobs(int noOfFetchedJobs) {
		this.noOfFetchedJobs = noOfFetchedJobs;
	}

	public int getNoOfFinishedJobs() {
		return noOfFinishedJobs;
	}

	public void setNoOfFinishedJobs(int noOfFinishedJobs) {
		this.noOfFinishedJobs = noOfFinishedJobs;
	}

	public int getNoOfFailedJobs() {
		return noOfFailedJobs;
	}

	public void setNoOfFailedJobs(int noOfFailedJobs) {
		this.noOfFailedJobs = noOfFailedJobs;
	}
	
}
