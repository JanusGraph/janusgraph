package com.thinkaurelius.titan.util.stats;

import java.io.File;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Preconditions;

public enum MetricManager {
    INSTANCE;
    
    private static final Logger log =
            LoggerFactory.getLogger(MetricManager.class);
    
    private final MetricRegistry registry   = new MetricRegistry();
    private ConsoleReporter consoleReporter = null;
    private CsvReporter csvReporter         = null;
    private JmxReporter jmxReporter         = null;

//    private static final long DEFAULT_CONSOLE_REPORTER_INTERVAL_S = 1L;
//    
//    private MetricManager() {
//        System.out.println("Enabled Metrics console reporter");
//        addConsoleReporter(DEFAULT_CONSOLE_REPORTER_INTERVAL_S);
//    }
    
    public MetricRegistry getRegistry() {
        return registry;
    }
    
    public synchronized void addConsoleReporter(long reportIntervalInSeconds) {
        Preconditions.checkArgument(null == consoleReporter);
        
        consoleReporter = ConsoleReporter.forRegistry(getRegistry()).build();
        consoleReporter.start(reportIntervalInSeconds, TimeUnit.SECONDS);
    }

    public synchronized void removeConsoleReporter() {
        if (null != consoleReporter)
            consoleReporter.stop();
        
        consoleReporter = null;
    }
    
    public synchronized void addCsvReporter(long reportIntervalInSeconds,
            File outputDir) {
        Preconditions.checkArgument(null == csvReporter);
        
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                log.warn("Failed to create CSV metrics dir {}", outputDir);
            }
        }
        
        csvReporter = CsvReporter.forRegistry(getRegistry()).build(outputDir);
        csvReporter.start(reportIntervalInSeconds, TimeUnit.SECONDS);
    }
    
    public synchronized void removeCsvReporter() {
        if (null != csvReporter)
            csvReporter.stop();
        
        csvReporter = null;
    }
    
    public synchronized void addJmxReporter() {
        Preconditions.checkArgument(null == jmxReporter);
        
        jmxReporter = JmxReporter.forRegistry(getRegistry()).build();
        jmxReporter.start();
    }
    
    public synchronized void removeJmxReporter() {
        if(null != jmxReporter)
            jmxReporter.stop();
        
        jmxReporter = null;
    }
    
    public void removeAllReporters() {
        removeConsoleReporter();
        removeCsvReporter();
        removeJmxReporter();
    }
}
