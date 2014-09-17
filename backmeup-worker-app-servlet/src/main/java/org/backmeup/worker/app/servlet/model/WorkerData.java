package org.backmeup.worker.app.servlet.model;

import org.backmeup.worker.WorkerCore.WorkerState;

public class WorkerData {
	private WorkerState workerState;
	private int noOfCurrentJobs;
	private int noOfMaximumJobs;
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
		return noOfCurrentJobs;
	}

	public void setNoOfCurrentJobs(int noOfCurrentJobs) {
		this.noOfCurrentJobs = noOfCurrentJobs;
	}

	public int getNoOfMaximumJobs() {
		return noOfMaximumJobs;
	}

	public void setNoOfMaximumJobs(int noOfMaximumJobs) {
		this.noOfMaximumJobs = noOfMaximumJobs;
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
