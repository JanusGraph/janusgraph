package com.thinkaurelius.titan.diskstorage.cassandra.utils;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class starts a Thrift CassandraDaemon inside the current JVM.
 * The only substantial use for this class is in testing at the moment.
 *
 * @author Dan LaRocque <dalaro@hopcount.org>
 */
public class CassandraDaemonWrapper {

    private static volatile boolean started = false;

    private static final Logger log = LoggerFactory.getLogger(CassandraDaemonWrapper.class);

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

        /*
         * This main method doesn't block for any substantial length of time. It
         * creates and starts threads and returns in relatively short order.
         */
        CassandraDaemon.main(new String[0]);

        liveCassandraYamlPath = cassandraYamlPath;

        started = true;
    }
    
    public static synchronized boolean isStarted() {
        return started;
    }
}
