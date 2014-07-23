package org.backmeup.worker.app.gui;

import java.awt.BorderLayout;
import java.awt.EventQueue;
import java.awt.FlowLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;

import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.SwingWorker;
import javax.swing.Timer;
import javax.swing.border.EmptyBorder;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import net.miginfocom.swing.MigLayout;

import org.backmeup.worker.WorkerCore;
import org.backmeup.worker.app.gui.components.BarMeter;
import org.backmeup.worker.app.gui.logging.LogTextAppender;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.awt.Color;
import java.awt.SystemColor;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

public class WorkerFrame extends JFrame {
	private static final Logger logger = LoggerFactory.getLogger(WorkerFrame.class);
	
	private static final long serialVersionUID = 1L;
	private static final int TAB_CONFIG = 2;
	
	private int noOfCurrentJobs;
	private int noOfFinishedJobs;
	private int noOfFetchedJobs;
	private int noOfFailedJobs;
	
	private JPanel contentPane;
	private JTabbedPane tabbedPane;
	private JPanel pOverview;
	private JPanel panel_1;
	private JLabel label;
	private JLabel lblWorkerState;
	private JPanel panel_2;
	private JLabel lblcurrentjobs;
	private JLabel lblCurrentJobsCount;
	private JPanel pCurrentJobsBar;
	private JLabel lblFetchedJobs;
	private JLabel lblFinishedJobsCount;
	private JPanel pFinishedJobsBar;
	private JLabel lblFinishedJobs;
	private JLabel lblFailedJobs;
	private JLabel lblFetchedJobsCount;
	private JPanel pFetchedJobsBar;
	private JLabel lblFailedJobsCount;
	private JPanel pFailedJobsBar;
	private JPanel pLogs;
	private JPanel pConfig;
	private boolean configLoaded = false;
	private JScrollPane scConfig;
	private JTextArea txtConfig;
	/**
	 * @wbp.nonvisual location=841,189
	 */
	private final Timer timer = new Timer(0, (ActionListener) null);
	/**
	 * @wbp.nonvisual location=825,109
	 */
	private final WorkerCore workerCore = new WorkerCore();
	private boolean workerInitialized = false;
	
	private BarMeter bmCurrentJobs;
	private BarMeter bmFinishedJobs;
	private BarMeter bmFetchedJobs;
	private BarMeter bmFailedJobs;
	private JScrollPane scLogs;
	private JTextArea txtLogs;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					WorkerFrame frame = new WorkerFrame();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public WorkerFrame() {
		addWindowListener(new WindowAdapter() {
			@Override
			public void windowClosed(WindowEvent arg0) {
				shutdownWroker();
			}
		});
		setTitle("Backmeup Worker");
		timer.addActionListener(new ActionListener() {
			public void actionPerformed(ActionEvent arg0) {
				timerElapsed(arg0);
			}
		});
		timer.setDelay(1000);
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(100, 100, 450, 250);
		contentPane = new JPanel();
		contentPane.setBorder(new EmptyBorder(5, 5, 5, 5));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);
		
		tabbedPane = new JTabbedPane(JTabbedPane.TOP);
		tabbedPane.addChangeListener(new ChangeListener() {
			public void stateChanged(ChangeEvent e) {
				tabChanged(e);
			}
		});
		contentPane.add(tabbedPane, BorderLayout.CENTER);
		
		pOverview = new JPanel();
		tabbedPane.addTab("Overview", null, pOverview, null);
		pOverview.setLayout(new BorderLayout(0, 0));
		
		panel_1 = new JPanel();
		pOverview.add(panel_1, BorderLayout.NORTH);
		panel_1.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
		
		label = new JLabel("Worker State:");
		panel_1.add(label);
		
		lblWorkerState = new JLabel("Offline");
		panel_1.add(lblWorkerState);
		
		panel_2 = new JPanel();
		pOverview.add(panel_2, BorderLayout.CENTER);
		panel_2.setLayout(new MigLayout("", "[][][grow]", "[][][][]"));
		
		lblcurrentjobs = new JLabel("Current Jobs:");
		panel_2.add(lblcurrentjobs, "cell 0 0");
		
		lblCurrentJobsCount = new JLabel("1");
		panel_2.add(lblCurrentJobsCount, "cell 1 0");
		
		pCurrentJobsBar = new JPanel();
		panel_2.add(pCurrentJobsBar, "cell 2 0,grow");
		pCurrentJobsBar.setLayout(new BorderLayout(0, 0));
		
		bmCurrentJobs = new BarMeter();
		bmCurrentJobs.setBorderColor(SystemColor.control);
		bmCurrentJobs.setBackgroundColor(SystemColor.control);
		bmCurrentJobs.setBackground(SystemColor.control);
		bmCurrentJobs.setValue(50);
		pCurrentJobsBar.add(bmCurrentJobs);
		
		lblFetchedJobs = new JLabel("Fetched Jobs:");
		panel_2.add(lblFetchedJobs, "cell 0 1");
		
		lblFinishedJobsCount = new JLabel("1");
		panel_2.add(lblFinishedJobsCount, "cell 1 2");
		
		pFinishedJobsBar = new JPanel();
		panel_2.add(pFinishedJobsBar, "cell 2 2,grow");
		pFinishedJobsBar.setLayout(new BorderLayout(0, 0));
		
		bmFinishedJobs = new BarMeter();
		bmFinishedJobs.setValue(33);
		bmFinishedJobs.setBorderColor(SystemColor.control);
		bmFinishedJobs.setBackgroundColor(SystemColor.control);
		bmFinishedJobs.setBackground(SystemColor.control);
		pFinishedJobsBar.add(bmFinishedJobs);
		
		lblFinishedJobs = new JLabel("Finished Jobs:");
		panel_2.add(lblFinishedJobs, "cell 0 2");
		
		lblFetchedJobsCount = new JLabel("1");
		panel_2.add(lblFetchedJobsCount, "cell 1 1");
		
		pFetchedJobsBar = new JPanel();
		panel_2.add(pFetchedJobsBar, "cell 2 1,grow");
		pFetchedJobsBar.setLayout(new BorderLayout(0, 0));
		
		bmFetchedJobs = new BarMeter();
		bmFetchedJobs.setValue(10);
		bmFetchedJobs.setForegroundColor(Color.RED);
		bmFetchedJobs.setBorderColor(SystemColor.control);
		bmFetchedJobs.setBackgroundColor(SystemColor.control);
		pFetchedJobsBar.add(bmFetchedJobs, BorderLayout.CENTER);
		
		lblFailedJobs = new JLabel("Failed Jobs:");
		panel_2.add(lblFailedJobs, "cell 0 3");
		
		lblFailedJobsCount = new JLabel("1");
		panel_2.add(lblFailedJobsCount, "cell 1 3");
		
		pFailedJobsBar = new JPanel();
		panel_2.add(pFailedJobsBar, "cell 2 3,grow");
		pFailedJobsBar.setLayout(new BorderLayout(0, 0));
		
		bmFailedJobs = new BarMeter();
		bmFailedJobs.setValue(10);
		bmFailedJobs.setBorderColor(SystemColor.control);
		bmFailedJobs.setBackgroundColor(SystemColor.control);
		pFailedJobsBar.add(bmFailedJobs, BorderLayout.CENTER);
		
		pLogs = new JPanel();
		tabbedPane.addTab("Logs", null, pLogs, null);
		pLogs.setLayout(new BorderLayout(0, 0));
		
		scLogs = new JScrollPane();
		pLogs.add(scLogs, BorderLayout.CENTER);
		
		txtLogs = new JTextArea();
		txtLogs.setEditable(false);
		scLogs.setViewportView(txtLogs);
		
		pConfig = new JPanel();
		tabbedPane.addTab("Config", null, pConfig, null);
		pConfig.setLayout(new BorderLayout(0, 0));
		
		scConfig = new JScrollPane();
		pConfig.add(scConfig, BorderLayout.CENTER);
		
		txtConfig = new JTextArea();
		txtConfig.setEditable(false);
		scConfig.setViewportView(txtConfig);
		
		intializeLogger();
		startWorker();
		timer.start();
	}
	
	private void intializeLogger() {
		LogTextAppender logAppender = new LogTextAppender(txtLogs);
		logAppender.start();
		
	}

	private void startWorker() {
		if (!workerInitialized) {
			new SwingWorker<Void, Void>() {
				@Override
				protected Void doInBackground() throws Exception {
					logger.info("Starting backmeup worker core");

					logger.info("Initializing worker");
					workerCore.initialize();
					logger.info("Initializing worker done.");

					logger.info("Starting worker");
					workerCore.start();
					logger.info("Starting worker done.");

					logger.info("Backmeup worker core startet");
					return null;
				}

			}.execute();
		}
	}
	
	private void shutdownWroker() {
		if(workerCore != null) {
			workerCore.shutdown();
		}
		
	}

	private void tabChanged(ChangeEvent e) {
		 JTabbedPane sourceTabbedPane = (JTabbedPane) e.getSource();
	     int index = sourceTabbedPane.getSelectedIndex();
	     if(index == TAB_CONFIG) {
	    	 if(!configLoaded) {
	    		 new SwingWorker<Void, String>() {
					@Override
					protected Void doInBackground() throws Exception {
						Properties workerProperties = new Properties();
						ClassLoader loader = Thread.currentThread().getContextClassLoader();
						workerProperties.load(loader.getResourceAsStream("backmeup-worker.properties"));


						StringBuilder sb = new StringBuilder();
						SortedSet<String> configLines = new TreeSet<>();

						for (Map.Entry<Object, Object> e : workerProperties.entrySet()) {
							String key = (String) e.getKey();
							String value = (String) e.getValue();

							String line = key + ": " + value + "\n";
							configLines.add(line);
						}
						
						for(String line : configLines) {
							sb.append(line);
						}
						
						publish(sb.toString());
						return null;
						
					}
	    			 
	                @Override
	                protected void process(List<String> chunks) {
	                	txtConfig.setText(chunks.get(0));
	                	configLoaded = true;
	                }
	                 
	             }.execute();
	    	 }
	     }
		
	}
	
	private void timerElapsed(ActionEvent ae) {
		EventQueue.invokeLater(new Runnable() {
			
			@Override
			public void run() {
				updateValuesFromWorker();
				updateControls();				
			}
		});		
	}

	private void updateValuesFromWorker() {
		lblWorkerState.setText(workerCore.getCurrentState().toString());
		
		noOfCurrentJobs = workerCore.getNoOfCurrentJobs();
		noOfFinishedJobs = workerCore.getNoOfFinishedJobs();
		noOfFetchedJobs = workerCore.getNoOfFetchedJobs();
//		noOfFailedJobs = workerCore.getNoOfFailedJobs();
		noOfFailedJobs = 0;
		
	}
	
	private void updateControls() {
		lblCurrentJobsCount.setText(noOfCurrentJobs + "");
		lblFinishedJobsCount.setText(noOfFinishedJobs + "");
		lblFetchedJobsCount.setText(noOfFetchedJobs + "");
		lblFailedJobsCount.setText(noOfFailedJobs + "");
		
		List<Integer> values = new ArrayList<>();
		values.add(noOfCurrentJobs);
		values.add(noOfFinishedJobs);
		values.add(noOfFetchedJobs);
		values.add(noOfFailedJobs);
		
		int maxValue = Collections.max(values);
		
		bmCurrentJobs.setMaximum(maxValue);
		bmCurrentJobs.setValue(noOfCurrentJobs);
		
		bmFetchedJobs.setMaximum(maxValue);
		bmFetchedJobs.setValue(noOfFetchedJobs);
		
		bmFinishedJobs.setMaximum(maxValue);
		bmFinishedJobs.setValue(noOfFinishedJobs);
		
		bmFailedJobs.setMaximum(maxValue);
		bmFailedJobs.setValue(noOfFailedJobs);
	}

}
