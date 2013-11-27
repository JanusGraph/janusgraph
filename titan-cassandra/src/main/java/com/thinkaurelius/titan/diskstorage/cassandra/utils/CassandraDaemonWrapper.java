package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * This class starts a Thrift CassandraDaemon inside the current JVM.
 * The only substantial use for this class is in testing at the moment.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraDaemonWrapper {

    private static final Logger log = LoggerFactory.getLogger(CassandraDaemonWrapper.class);

    private static String liveCassandraYamlPath;
    
    private static long refcount = 0;

    public static synchronized void start(String cassandraYamlPath) {
        if (0 < refcount++) {
            if (null != cassandraYamlPath && !cassandraYamlPath.equals(liveCassandraYamlPath)) {
                log.warn("Can't start in-process Cassandra instance " +
                         "with yaml path {} because an instance was " +
                         "previously started with yaml path {}",
                         cassandraYamlPath, liveCassandraYamlPath);
            }

            return;
        }

        log.debug("Current working directory: {}", System.getProperty("user.dir"));

        System.setProperty("cassandra.config", cassandraYamlPath);
        // Prevent Cassandra from closing stdout/stderr streams
        System.setProperty("cassandra-foreground", "yes");
        // Prevent Cassandra from overwriting Log4J configuration
        System.setProperty("log4j.defaultInitOverride", "false");

        /*
         * This main method doesn't block for any substantial length of time. It
         * creates and starts threads and returns in relatively short order.
         */
        CassandraDaemon.main(new String[0]);

        liveCassandraYamlPath = cassandraYamlPath;
    }
    
    public static synchronized boolean isStarted() {
        return 0 < refcount;
    }
    
    public static synchronized void stop() {
        if (0 >= refcount--) {
            log.warn("Can't stop in-process Cassandra instance because it is not running");
            return;
        }
        
        if (0 == refcount) {
            log.info("Stopping in-process Cassandra daemon");
            CassandraDaemon.stop(null);
            try {
                CommitLog.instance.shutdownBlocking();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            CommitLog.instance.sync();
            terminatePeriodicCommitLogThread();
            MessagingService.instance().shutdown();
        }
    }
    
    private static void terminatePeriodicCommitLogThread() {
        ThreadGroup root = getRootThreadGroup();
        
        if (null == root)
            return; // Shouldn't happen, but give up if it does
        
        int tc = 4096;
        Thread[] threads = new Thread[tc];
        int enumerated = root.enumerate(threads);
        if (enumerated == tc)
            return; // Wait it out, the perodic commit syncer will die eventually
        
        for (int i = 0; i < enumerated; i++) {
            final Thread t = threads[i];
            if (t.getName().equals("PERIODIC-COMMIT-LOG-SYNCER")) {
                installUncaughtInterruptSwallower(t);
                t.interrupt();
            }
        }
    }
    
    private static ThreadGroup getRootThreadGroup() {
        ThreadGroup g = Thread.currentThread().getThreadGroup();
        if (null == g)
            return null;
        
        ThreadGroup next = g.getParent();
        while (null != next) {
            g = next;
            next = next.getParent();
        }
        
        return g;
    }

    // this is not the greatest idea i've ever had
    private static void installUncaughtInterruptSwallower(final Thread t) {
        
        t.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
            @Override
            public void uncaughtException(Thread actualThread, Throwable a) {
                if (t.equals(actualThread) && a instanceof AssertionError) {
                    Throwable cause = a.getCause();
                    if (null != cause && cause instanceof InterruptedException)
                        return;
                }
                
                log.error("Uncaught exception", a);
            }
        });
        
        log.debug("Installed uncaught exception handler on {}", t);
    }
}
