package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import java.lang.Thread.UncaughtExceptionHandler;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts a Thrift CassandraDaemon inside the current JVM. This class
 * supports testing and shouldn't be used in production.
 *
 * This class starts Cassandra on the first invocation of
 * {@link CassandraDaemonWrapper#start(String)} in the life of the JVM.
 * Invocations after the first have no effect except that they may log a
 * warning.
 *
 * When the thread that first called {@code #start(String)} dies, a daemon
 * thread returns from {@link Thread#join()} and kills all embedded Cassandra
 * threads in the JVM.
 *
 * This class once supported consecutive, idempotent calls to start(String) so
 * long as the argument was the same in each invocation. It also once used
 * refcounting to kill Cassandra's non-daemon threads once stop() was called as
 * many times as start(). Some of Cassandra's background threads and statics
 * can't be easily reset to allow a restart inside the same JVM, so this was
 * intended as a one-use thing. However, this interacts poorly with the new
 * KCVConfiguration system in titan-core. When KCVConfiguration is in use, core
 * starts and stops each backend at least twice in the course of opening a
 * single database instance. So the old refcounting and killing approach is out.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraDaemonWrapper {

    private static final Logger log =
            LoggerFactory.getLogger(CassandraDaemonWrapper.class);

    private static String activeConfig;

    private static boolean started;

    public static synchronized void start(String config) {

        if (started) {
            if (null != config && !config.equals(activeConfig)) {
                log.warn("Can't start in-process Cassandra instance " +
                         "with yaml path {} because an instance was " +
                         "previously started with yaml path {}",
                         config, activeConfig);
            }

            return;
        }

        started = true;

        log.debug("Current working directory: {}", System.getProperty("user.dir"));

        System.setProperty("cassandra.config", config);
        // Prevent Cassandra from closing stdout/stderr streams
        System.setProperty("cassandra-foreground", "yes");
        // Prevent Cassandra from overwriting Log4J configuration
        System.setProperty("log4j.defaultInitOverride", "false");

        log.info("Starting cassandra with {}", config);

        /*
         * This main method doesn't block for any substantial length of time. It
         * creates and starts threads and returns in relatively short order.
         */
        CassandraDaemon.main(new String[0]);

        activeConfig = config;

        new CassandraKiller(Thread.currentThread()).start();
    }

    public static synchronized boolean isStarted() {
        return started;
    }

    public static void stop() {
        // Do nothing
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

    private static class CassandraKiller extends Thread {

        private final Thread protector;

        public CassandraKiller(Thread protector) {
            super();
            this.protector = protector;
            this.setDaemon(true);
            this.setName(getClass().getSimpleName());
        }

        @Override
        public void run() {

            try {
                log.info("Joining thread {}", protector.getName());
                protector.join();
            } catch (InterruptedException e) {
                log.info("Cassandra killer aborting due to interrupt", e);
                return;
            }

            log.info("Killing embedded Cassandra threads because {} died", protector);

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
}
