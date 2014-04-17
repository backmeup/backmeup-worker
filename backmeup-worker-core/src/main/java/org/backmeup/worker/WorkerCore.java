package org.backmeup.worker;

import org.backmeup.plugin.Plugin;

public class WorkerCore {
	private int maxWorkerThreads;
	private int currentWorkerThreads;
	private Boolean initialized;
	private WorkerState currentState;
	
	private Plugin plugins;
	
	// Getters and setters
	
	public WorkerState getCurrentState() {
		return currentState;
	}

	private void setCurrentState(WorkerState currentState) {
		this.currentState = currentState;
	}
	
	public int getNoOfCurrentJobs() {
		throw new UnsupportedOperationException();
	}

	public int getNoOfFinishedJobs() {
		throw new UnsupportedOperationException();
	}
	
	public int getNoOfFetchedJobs() {
		throw new UnsupportedOperationException();
	}
	
	public int getNoOfFailedJobs() {
		throw new UnsupportedOperationException();
	}
	
	// Constructor
	
	public WorkerCore() {
		
	}
	
	// Public Methods
	
	public void initialize() {
		
	}
	
	public void start() {
		
	}
	
	public void shutdown() {
		
	}
	
	// Nested classes and enums
	
	private enum WorkerState {
		Offline, // Not connected to dependent services
		Idle,    // No jobs to execute
		Busy    // Jobs are currently running on worker
	}
}
