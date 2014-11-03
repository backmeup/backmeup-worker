package org.backmeup.worker.job.receiver;

import java.util.EventObject;

public class JobReceivedEvent extends EventObject {
	private static final long serialVersionUID = 6959348412326443090L;
	
	protected Long jobId;

	public JobReceivedEvent(Object obj, Long jobId) {
		super(obj);
		this.jobId = jobId;
	}

	public Long getJobId() {
		return jobId;
	}
}
