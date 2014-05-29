package org.backmeup.worker.job.threadpool;

import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ObservableThreadPoolExecutor extends ThreadPoolExecutor {
	private final Vector<ThreadPoolListener> listeners = new Vector<>();
	
	// Constructors -----------------------------------------------------------

	public ObservableThreadPoolExecutor(int corePoolSize, int maximumPoolSize,
			long keepAliveTime, TimeUnit unit, BlockingQueue<Runnable> workQueue) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
	}
	
	public ObservableThreadPoolExecutor(int corePoolSize,
			int maximumPoolSize, long keepAliveTime, TimeUnit unit,
			BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
		super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
	}
	
	// Hook methods -----------------------------------------------------------

	@Override
	protected void beforeExecute(Thread t, Runnable r) {
		super.beforeExecute(t, r);
		@SuppressWarnings("unchecked")
		Vector<ThreadPoolListener> listenerClone = (Vector<ThreadPoolListener>) listeners.clone();
		for(ThreadPoolListener l : listenerClone){
			l.beforeExecute(t, r);
		}
	}
	
	@Override
	protected void afterExecute(Runnable r, Throwable t) {
		super.afterExecute(r, t);
		@SuppressWarnings("unchecked")
		Vector<ThreadPoolListener> listenerClone = (Vector<ThreadPoolListener>) listeners.clone();
		for(ThreadPoolListener l : listenerClone){
			l.afterExecute(r, t);
		}
	}
	
	@Override
	protected void terminated() {
		super.terminated();
		@SuppressWarnings("unchecked")
		Vector<ThreadPoolListener> listenerClone = (Vector<ThreadPoolListener>) listeners.clone();
		for(ThreadPoolListener l : listenerClone){
			l.terminated();
		}
	}
	
	// Registration methods for event -----------------------------------------
	
	public void addThreadPoolListener(ThreadPoolListener listener){
		listeners.add(listener);
	}

	public void removeThreadPoolListener(ThreadPoolListener listener){
		listeners.remove(listener);
	}

}
