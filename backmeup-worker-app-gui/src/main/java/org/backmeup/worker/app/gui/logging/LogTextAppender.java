package org.backmeup.worker.app.gui.logging;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import javax.swing.JTextArea;
import javax.swing.SwingUtilities;

import org.backmeup.worker.WorkerException;
import org.slf4j.LoggerFactory;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.encoder.EchoEncoder;
import ch.qos.logback.core.encoder.Encoder;

public class LogTextAppender extends AppenderBase<ILoggingEvent> {
    private static final String ROOT_LOGGER_NAME = "ROOT";
    private static final String OUTPUT_ENCODING = "UTF-8";
    
    private Encoder<ILoggingEvent> encoder = new EchoEncoder<>();
    private ByteArrayOutputStream out = new ByteArrayOutputStream();

    private JTextArea jTextArea;

    public LogTextAppender(JTextArea jTextArea) {
        this.jTextArea = jTextArea;
        LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
        lc.getLogger(ROOT_LOGGER_NAME).addAppender(this);
        setContext(lc);
    }

    @Override
    public void start() {
        try {
            encoder.init(out);
        } catch (IOException e) {
            throw new WorkerException("Failed to initialze logger", e);
        }
        super.start();
    }

    @Override
    public void append(ILoggingEvent event) {
        try {
            encoder.doEncode(event);
            out.flush();
            final String line = out.toString(OUTPUT_ENCODING);

            SwingUtilities.invokeLater(new Runnable() {
                @Override
                public void run() {
                    if (jTextArea != null) {
                        jTextArea.append(line);
                    }
                }
            });
            out.reset();
        } catch (IOException e) {
            throw new WorkerException(e);
        }
    }
}
