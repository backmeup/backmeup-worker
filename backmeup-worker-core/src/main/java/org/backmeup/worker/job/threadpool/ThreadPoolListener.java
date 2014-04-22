package org.backmeup.worker.job.threadpool;

public interface ThreadPoolListener {
	void beforeExecute(Thread t, Runnable r);
	void afterExecute(Runnable r, Throwable t);
	void terminated();
}
