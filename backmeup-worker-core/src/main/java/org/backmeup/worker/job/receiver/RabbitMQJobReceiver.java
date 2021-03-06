package org.backmeup.worker.job.receiver;

import java.io.IOException;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.backmeup.model.exceptions.BackMeUpException;
import org.backmeup.worker.utils.ByteUtils;
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
    private static final int DELAY_INTERVAL = 500;

    private static final Logger LOGGER = LoggerFactory.getLogger(RabbitMQJobReceiver.class);

    private final String mqName;
    private final String mqHost;

    private boolean initialized;

    private final AtomicReference<Connection> mqConnection;
    private final AtomicReference<Channel> mqChannel;
    private final AtomicInteger mqTimeout;

    private final AtomicReference<Thread> receiverThread;
    private final AtomicBoolean stopReceiver;
    private final AtomicBoolean pauseReceiver;
    private final AtomicInteger pauseInterval;

    private final Vector<JobReceivedListener> listeners;

    public RabbitMQJobReceiver(String mqHostAdr, String mqName, int waitInterval) {
        this.mqName = mqName;
        this.mqHost = mqHostAdr;
        this.mqTimeout = new AtomicInteger(waitInterval);

        this.stopReceiver = new AtomicBoolean(false);
        this.pauseReceiver = new AtomicBoolean(false);
        this.pauseInterval = new AtomicInteger(waitInterval);

        this.mqChannel = new AtomicReference<>(null);
        this.mqConnection = new AtomicReference<>(null);
        this.receiverThread = new AtomicReference<>(null);

        this.initialized = false;

        this.listeners = new Vector<>();
    }


    // Properties -------------------------------------------------------------

    public boolean isRunning() {
        return receiverThread.get() != null;
    }

    public boolean isPaused() {
        return pauseReceiver.get();
    }

    public void setPaused(boolean paused) {
        pauseReceiver.set(paused);
    }

    // Methods ----------------------------------------------------------------

    public void initialize() {
        // Connect to the message queue
        LOGGER.info("Connecting to the message queue");

        try {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(mqHost);
            mqConnection.set(factory.newConnection());
            mqChannel.set(mqConnection.get().createChannel());
            mqChannel.get().queueDeclare(mqName, false, false, false, null);

            initialized = true;
        } catch (IOException e) {
            throw new BackMeUpException(e);
        }

    }

    public void start() {
        if (!initialized) {
            throw new IllegalStateException("Cannot start: receiver is not initialized");
        }

        if (isRunning()) {
            throw new IllegalStateException("Cannot start: receiver is already running");
        }

        receiverThread.set(new Thread(new Runnable() {
            @Override
            public void run() {
                LOGGER.info("Starting message queue receiver");

                QueueingConsumer consumer = new QueueingConsumer(mqChannel.get());

                try {
                    mqChannel.get().basicConsume(mqName, true, consumer);
                    while (!stopReceiver.get()) {
                        if(!pauseReceiver.get()) {
                            try {
                                QueueingConsumer.Delivery delivery = consumer.nextDelivery(mqTimeout.get());
                                if (delivery != null) {
                                    byte[] body = delivery.getBody();

                                    Long jobId = ByteUtils.bytesToLong(body);
                                    LOGGER.info("Received job with id: " + jobId);

                                    fireEvent(new JobReceivedEvent(this, jobId));

                                    // Delay further receiving to give the callback listener a chance
                                    // to react on (e.g. pause or stop)
                                    Thread.sleep(DELAY_INTERVAL);
                                }
                            } catch (Exception ex) {
                                LOGGER.error("Failed to receive job", ex);
                            }
                        }
                        if(pauseReceiver.get()) {
                            try {
                                Thread.sleep(pauseInterval.get());
                            } catch (InterruptedException e) {
                                LOGGER.error("",e);
                            }
                        }
                    }

                    LOGGER.info("Stopping message queue receiver");

                    mqChannel.get().close();
                    mqConnection.get().close();

                } catch (IOException e) {
                    // Should only happen if message queue is down
                    LOGGER.error("Message queue down", e);
                    throw new BackMeUpException(e);
                }

                LOGGER.info("Message queue receiver stopped");

                stopReceiver.set(false);
                receiverThread.set(null);
            }
        }));

        receiverThread.get().start();
    }

    public void stop() {
        stopReceiver.set(true);
        try {
            // Wait for thread to complete
            receiverThread.get().join();
        } catch (InterruptedException e) {
            LOGGER.error("", e);
            throw new BackMeUpException(e);
        }
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
