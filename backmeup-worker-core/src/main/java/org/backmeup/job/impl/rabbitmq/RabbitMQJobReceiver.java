package org.backmeup.job.impl.rabbitmq;

import java.io.IOException;

import org.backmeup.job.impl.BackupJobRunner;
import org.backmeup.keyserver.client.KeyserverFacade;
import org.backmeup.model.BackupJob;
import org.backmeup.model.serializer.JsonSerializer;
import org.backmeup.plugin.Plugin;
import org.backmeup.plugin.api.storage.Storage;
import org.backmeup.service.client.BackmeupServiceFacade;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.QueueingConsumer;

/**
 * A Receiver class that listens to a RabbitMQ message queue and executes Backup
 * jobs that get sent across the wire.
 * 
 */
public class RabbitMQJobReceiver {
	private final Logger logger = LoggerFactory.getLogger(RabbitMQJobReceiver.class);
	
	private String indexHost;
	private Integer indexPort;
	
	private String jobTempDir;
	private String backupName;
	
	private String mqName;
	private String mqHost;
	
	private Connection mqConnection;
	private Channel mqChannel;
	
	private Plugin plugins;
	private KeyserverFacade keyserver;
	private BackmeupServiceFacade service;

	private boolean listening;


	public RabbitMQJobReceiver(String mqHostAdr, String mqName, String indexHost, Integer indexPort, String backupName, String jobTempDir, Plugin plugins, KeyserverFacade keyserver, BackmeupServiceFacade service) throws IOException {
		this.mqName = mqName;
		this.mqHost = mqHostAdr;
		this.indexHost = indexHost;
		this.indexPort = indexPort;
		this.backupName = backupName;
		this.jobTempDir = jobTempDir;
		
		this.plugins = plugins;
		this.keyserver = keyserver;
		this.service = service;
		
		this.listening = false;

		// Connect to the message queue
		logger.info("Connecting to the message queue");
		ConnectionFactory factory = new ConnectionFactory();
		factory.setHost(mqHost);

		mqConnection = factory.newConnection();
		mqChannel = mqConnection.createChannel();
		mqChannel.queueDeclare(mqName, false, false, false, null);
	}

	public boolean isListening() {
		return listening;
	}

	public void start() {
		if (!listening) {
			listening = true;

			Thread t = new Thread(new Runnable() {
				@Override
				public void run() {
					try {
						logger.info("Starting message queue receiver");
						int timeoutInMS = 500;
						QueueingConsumer consumer = new QueueingConsumer(mqChannel);
						mqChannel.basicConsume(mqName, true, consumer);

						while (listening) {
							try {
								QueueingConsumer.Delivery delivery = consumer.nextDelivery(timeoutInMS);
								if(delivery != null) {
									String message = new String(delivery.getBody());
									logger.info("Received: " + message);

									BackupJob job = JsonSerializer.deserialize(message, BackupJob.class);

//									Storage storage = new LocalFilesystemStorage();
									Storage storage = null;
									BackupJobRunner runner = new BackupJobRunner(plugins, keyserver, service, indexHost, indexPort, jobTempDir, backupName);
									runner.executeBackup(job, storage);
								}
							} catch (Exception ex) {
								logger.error("failed to process job", ex);
							}
						}

						logger.info("Stopping message queue receiver");
						
						mqChannel.close();
						mqConnection.close();
						
						plugins.shutdown();
						
						logger.info("Message queue receiver stopped");
					} catch (IOException e) {
						// Should only happen if message queue is down
						logger.error("message queue down", e);
						throw new RuntimeException(e);
					}
				}
			});

			t.start();
		}
	}

	public void stop() {
		listening = false;
	}

}
