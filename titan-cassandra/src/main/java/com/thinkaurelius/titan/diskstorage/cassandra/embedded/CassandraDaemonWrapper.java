package com.thinkaurelius.titan.diskstorage.cassandra.embedded;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

/**
 * This class starts a Thrift CassandraDaemon inside the current JVM.
 * The only substantial use for this class is in testing at the moment.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraDaemonWrapper {

    private static volatile boolean started = false;

    private static final Logger log = LoggerFactory.getLogger(CassandraDaemonWrapper.class);
    private static final ExecutorService daemonExec = Executors.newSingleThreadExecutor(new DaemonThreadFactory());

    private static String liveCassandraYamlPath;

    public static synchronized void start(String cassandraYamlPath) {
        if (started) {
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


        try {
            daemonExec.submit(new CassandraStarter()).get();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }

        liveCassandraYamlPath = cassandraYamlPath;

        started = true;
    }

    private static class CassandraStarter implements Runnable {
        @Override
        public void run() {
            CassandraDaemon.main(new String[0]);
        }
    }

    /**
     * Just like Executors.defaultThreadFactory(), except that it always returns
     * daemon threads.
     *
     * @author Dan LaRocque <dalaro@hopcount.org>
     */
    private static class DaemonThreadFactory implements ThreadFactory {

        private final ThreadFactory dfl =
                Executors.defaultThreadFactory();

        @Override
        public Thread newThread(Runnable r) {
            Thread t = dfl.newThread(r);
            t.setDaemon(true);
            return t;
        }

    }
}
