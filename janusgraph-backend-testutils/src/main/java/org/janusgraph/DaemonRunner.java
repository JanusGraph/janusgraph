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

package org.janusgraph;

import com.google.common.base.Joiner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

public abstract class DaemonRunner<S> {

    private static final Logger log =
            LoggerFactory.getLogger(DaemonRunner.class);

    private Thread killerHook;

    protected abstract String getDaemonShortName();

    protected abstract void killImpl(final S stat) throws IOException;

    protected abstract S startImpl() throws IOException;

    protected abstract S readStatusFromDisk();

    /**
     * Read daemon status from disk, then try to start the daemon
     * if the status file says it is stopped.  Do nothing if the
     * status read from disk says the daemon is running.
     *
     * After successfully starting the daemon (no exceptions from
     * {@link #startImpl()}, register a shutdown hook with the VM
     * that will call {@link org.janusgraph.DaemonRunner#killImpl} on shutdown.
     *
     * @return status representing the daemon, either just-started
     *         or already running
     */
    public synchronized S start() {
        S stat = readStatusFromDisk();

        if (stat != null) {
            log.info("{} already started", getDaemonShortName());
            return stat;
        }

        try {
            stat = startImpl();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        registerKillerHook(stat);

        return stat;
    }

    /**
     * Read daemon status from disk, then try to kill the daemon
     * if the status file says it is running.  Do nothing if the
     * status read from disk says the daemon is stopped.
     */
    public synchronized void stop() {
        S stat = readStatusFromDisk();

        if (null == stat) {
            log.info("{} is not running", getDaemonShortName());
            return;
        }

        killAndUnregisterHook(stat);
    }

    private synchronized void registerKillerHook(final S stat) {
        if (null != killerHook) {
            log.debug("Daemon killer hook already registered: {}", killerHook);
            return;
        }
        killerHook = new Thread(() -> killAndUnregisterHook(stat));
        Runtime.getRuntime().addShutdownHook(killerHook);
        log.debug("Registered daemon killer hook: {}", killerHook);
    }

    private synchronized void killAndUnregisterHook(final S stat) {

        try {
            killImpl(stat);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if (null != killerHook) {
            try {
                Runtime.getRuntime().removeShutdownHook(killerHook);
                log.debug("Unregistered killer hook: {}", killerHook);
            } catch (IllegalStateException e) {
                /* Can receive "java.lang.IllegalStateException: Shutdown in progress"
                 * when called from JVM shutdown (as opposed to called from the stop method).
                 */
                log.debug("Could not unregister killer hook: {}", killerHook.getName(), e);
            }
            killerHook = null;
        }
    }

    /**
     * Run the parameter as an external process. Returns if the command starts
     * without throwing an exception and returns exit status 0. Throws an
     * exception if there's any problem invoking the command or if it does not
     * return zero exit status.
     *
     * Blocks indefinitely while waiting for the command to complete.
     *
     * @param argv
     *            passed directly to {@link ProcessBuilder}'s constructor
     */
    public static void runCommand(String... argv) {

        final String cmd = Joiner.on(" ").join(argv);
        log.info("Executing {}", cmd);

        ProcessBuilder pb = new ProcessBuilder(argv);
        pb.redirectErrorStream(true);
        Process startup;
        try {
            startup = pb.start();
        } catch (IOException e) {
            throw new RuntimeException("Unable to start process in " + System.getProperty("user.dir") + ": " + e.getMessage(), e);
        }
        StreamLogger sl = new StreamLogger(startup.getInputStream());
        sl.setDaemon(true);
        sl.start();

        waitForProcessAndCheckStatus(cmd, startup);

        try {
            sl.join(1000L);
        } catch (InterruptedException e) {
            log.warn("Failed to cleanup stdin handler thread after running command \"{}\"", cmd, e);
        }
    }

    private static void waitForProcessAndCheckStatus(String cmd, Process startup) {
        try {
            int exitCode = startup.waitFor(); // wait for script to return
            if (0 == exitCode) {
                log.info("Command \"{}\" exited with status 0", cmd);
            } else {
                throw new RuntimeException("Command \"" + cmd + "\" exited with status " + exitCode);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /*
     * This could be retired in favor of ProcessBuilder.Redirect when we move to
     * source level 1.7.
     */
    private static class StreamLogger extends Thread {

        private final BufferedReader reader;
        private static final Logger log =
                LoggerFactory.getLogger(StreamLogger.class);

        private StreamLogger(InputStream is) {
            this.reader = new BufferedReader(new InputStreamReader(is));
        }

        @Override
        public void run() {
            String line;
            try {
                while (null != (line = reader.readLine())) {
                    log.info("> {}", line);
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }

                log.info("End of stream.");
            } catch (IOException e) {
                log.error("Unexpected IOException while reading stream {}", reader, e);
            }
        }
    }
}
