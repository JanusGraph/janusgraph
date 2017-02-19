// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.diskstorage.cassandra.utils;

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
 * KCVConfiguration system in janusgraph-core. When KCVConfiguration is in use, core
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
    }

    public static synchronized boolean isStarted() {
        return started;
    }

    public static void stop() {
        // Do nothing
    }
}
