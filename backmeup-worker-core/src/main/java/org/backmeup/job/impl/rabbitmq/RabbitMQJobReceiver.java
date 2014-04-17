package org.backmeup.job.impl.rabbitmq;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.backmeup.job.impl.JobReceivedEvent;
import org.backmeup.job.impl.JobReceivedListener;
import org.backmeup.job.impl.JobReceiver;
import org.backmeup.model.BackupJob;
import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.model.serializer.JsonSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * A Receiver class that listens to a RabbitMQ message queue and receives Backup
 * jobs that get sent across the wire.
 * 
 */
public class RabbitMQJobReceiver implements JobReceiver{
	private final Logger logger = LoggerFactory.getLogger(RabbitMQJobReceiver.class);

	private String mqName;
	private String mqHost;

	private Connection mqConnection;
	private Channel mqChannel;
	private AtomicInteger mqTimeout;

	private AtomicReference<Thread> receiverThread;
	private AtomicBoolean stopReceiver;
	private AtomicBoolean pauseReceiver;
	private AtomicInteger pauseInterval;
	
	private Vector<JobReceivedListener> listeners;

	public RabbitMQJobReceiver(String mqHostAdr, String mqName, int waitInterval) {
		this.mqName = mqName;
		this.mqHost = mqHostAdr;
		this.mqTimeout = new AtomicInteger(waitInterval);

		this.stopReceiver = new AtomicBoolean(false);
		this.pauseReceiver = new AtomicBoolean(false);
		this.pauseInterval = new AtomicInteger(waitInterval);
		
		this.listeners = new Vector<JobReceivedListener>();
	}

	
	// Properties -------------------------------------------------------------
	
	public boolean isRunning() {
		return receiverThread.get() != null;
	}
	
	public boolean isPaused() {
		return pauseReceiver.get();
	}
	
	// Methods ----------------------------------------------------------------
	
	public void initialize() {
		// Connect to the message queue
		logger.info("Connecting to the message queue");

		try {
			ConnectionFactory factory = new ConnectionFactory();
			factory.setHost(mqHost);
			mqConnection = factory.newConnection();
			mqChannel = mqConnection.createChannel();
			mqChannel.queueDeclare(mqName, false, false, false, null);
		} catch (IOException e) {
			throw new BackMeUpException(e);
		}

	}

	public void start() {
		if (isRunning()) {
			throw new IllegalStateException(
					"Cannot start: receiver is already running");
		}

		receiverThread.set(new Thread(new Runnable() {
			public void run() {
				logger.info("Starting message queue receiver");
				
				QueueingConsumer consumer = new QueueingConsumer(mqChannel);
				
				try {
					mqChannel.basicConsume(mqName, true, consumer);
					while (!stopReceiver.get()) {
						if(!pauseReceiver.get()) {
							try {
								QueueingConsumer.Delivery delivery = consumer.nextDelivery(mqTimeout.get());
								if (delivery != null) {
									String message = new String(delivery.getBody());
									logger.info("Job received: " + message);

									BackupJob job = JsonSerializer.deserialize(message, BackupJob.class);

									if (!stopReceiver.get()) {
										 fireEvent(new JobReceivedEvent(this, job));
									}
								}
							} catch (Exception ex) {
								logger.error("Failed to receive job", ex);
							}
						}
						if(pauseReceiver.get()) {
							try {
								Thread.sleep(pauseInterval.get());
							} catch (InterruptedException e) {
							}
						}
					}

					logger.info("Stopping message queue receiver");

					mqChannel.close();
					mqConnection.close();

				} catch (IOException e) {
					// Should only happen if message queue is down
					logger.error("Message queue down", e);
					throw new RuntimeException(e);
				}

				logger.info("Message queue receiver stopped");

				stopReceiver.set(false);
				receiverThread.set(null);
			}
		}));

		receiverThread.get().start();
	}

	public void stop() {
		stopReceiver.set(true);
	}
	
	public void pause() {
		pauseReceiver.set(true);
	}
	
	// Events -----------------------------------------------------------------
	
	protected void fireEvent(JobReceivedEvent jre){
		@SuppressWarnings("unchecked")
		Vector<JobReceivedListener> listenerClone = (Vector<JobReceivedListener>) listeners.clone();
		for(JobReceivedListener l : listenerClone){
			l.jobReceived(jre);
		}
	}

	public void addJobReceivedListener(JobReceivedListener listener){
		listeners.add(listener);
	}

	public void removeJobReceivedListener(JobReceivedListener listener){
		listeners.remove(listener);
	}
}
