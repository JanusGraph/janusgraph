package com.thinkaurelius.titan.util.stats;

import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import java.io.File;
import java.util.List;
import java.util.concurrent.TimeUnit;

public enum MetricManager {
    INSTANCE;
    
    private static final Logger log =
            LoggerFactory.getLogger(MetricManager.class);
    
    private final MetricRegistry registry   = new MetricRegistry();
    private ConsoleReporter consoleReporter = null;
    private CsvReporter csvReporter         = null;
    private JmxReporter jmxReporter         = null;
    private Slf4jReporter slf4jReporter     = null;

//    private static final long DEFAULT_CONSOLE_REPORTER_INTERVAL_S = 1L;
//    
//    private MetricManager() {
//        System.out.println("Enabled Metrics console reporter");
//        addConsoleReporter(DEFAULT_CONSOLE_REPORTER_INTERVAL_S);
//    }
    
    public MetricRegistry getRegistry() {
        return registry;
    }
    
    public synchronized void addConsoleReporter(long reportIntervalInMS) {
        if (null != consoleReporter) {
            log.debug("Metrics ConsoleReporter already active; not creating another");
            return;
        }
        
        consoleReporter = ConsoleReporter.forRegistry(getRegistry()).build();
        consoleReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);
    }

    public synchronized void removeConsoleReporter() {
        if (null != consoleReporter)
            consoleReporter.stop();
        
        consoleReporter = null;
    }
    
    public synchronized void addCsvReporter(long reportIntervalInMS,
            String output) {
        
        File outputDir = new File(output);
        
        if (null != csvReporter) {
            log.debug("Metrics CsvReporter already active; not creating another");
            return;
        }
        
        if (!outputDir.exists()) {
            if (!outputDir.mkdirs()) {
                log.warn("Failed to create CSV metrics dir {}", outputDir);
            }
        }
        
        csvReporter = CsvReporter.forRegistry(getRegistry()).build(outputDir);
        csvReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);
    }
    
    public synchronized void removeCsvReporter() {
        if (null != csvReporter)
            csvReporter.stop();
        
        csvReporter = null;
    }
    
    public synchronized void addJmxReporter(String domain, String agentId) {
        if (null != jmxReporter) {
            log.debug("Metrics JmxReporter already active; not creating another");
            return;
        }
        
        JmxReporter.Builder b = JmxReporter.forRegistry(getRegistry());
        
        if (null != domain) {
            b.inDomain(domain);
        }
        
        if (null != agentId) {
            List<MBeanServer> servs = MBeanServerFactory.findMBeanServer(agentId);
            if (null != servs && 1 == servs.size()) {
                b.registerWith(servs.get(0));
            } else {
                log.error("Metrics Slf4jReporter agentId {} does not resolve to a single MBeanServer", agentId);
            }
        }
        
        jmxReporter = b.build();
        jmxReporter.start();
    }
    
    public synchronized void removeJmxReporter() {
        if (null != jmxReporter)
            jmxReporter.stop();
        
        jmxReporter = null;
    }
    
    public synchronized void addSlf4jReporter(long reportIntervalInMS, String loggerName) {
        if (null != slf4jReporter) {
            log.debug("Metrics Slf4jReporter already active; not creating another");
            return;
        }
        
        Slf4jReporter.Builder b = Slf4jReporter.forRegistry(getRegistry());
        
        if (null != loggerName) {
            Logger l = LoggerFactory.getLogger(loggerName);
            if (null != l) {
                b.outputTo(l);
            } else {
                log.error("Logger with name {} could not be obtained", loggerName);
            }
        }
        
        slf4jReporter = b.build();
        slf4jReporter.start(reportIntervalInMS, TimeUnit.MILLISECONDS);
    }
    
    public synchronized void removeSlf4jReporter() {
        if (null != slf4jReporter)
            slf4jReporter.stop();
        
        slf4jReporter = null;
    }
    
    public void removeAllReporters() {
        removeConsoleReporter();
        removeCsvReporter();
        removeJmxReporter();
        removeSlf4jReporter();
    }
}
