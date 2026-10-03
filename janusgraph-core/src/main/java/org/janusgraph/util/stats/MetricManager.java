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

package org.janusgraph.util.stats;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.LockFreeExponentiallyDecayingReservoir;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Slf4jReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.graphite.Graphite;
import com.codahale.metrics.graphite.GraphiteReporter;
import com.codahale.metrics.jmx.JmxReporter;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;


/**
 * Singleton that contains and configures JanusGraph's {@code MetricRegistry}.
 */
public enum MetricManager {
    INSTANCE;

    private static final Logger log =
            LoggerFactory.getLogger(MetricManager.class);



    private static final MetricRegistry.MetricSupplier<Timer> TIMER_SUPPLIER =
        () -> new Timer(LockFreeExponentiallyDecayingReservoir.builder().build());
    private static final MetricRegistry.MetricSupplier<Histogram> HISTOGRAM_SUPPLIER =
        () -> new Histogram(LockFreeExponentiallyDecayingReservoir.builder().build());

    private final MetricRegistry registry     = new MetricRegistry();
    private ConsoleReporter consoleReporter   = null;
    private CsvReporter csvReporter           = null;
    private JmxReporter jmxReporter           = null;
    private Slf4jReporter slf4jReporter       = null;
    private GraphiteReporter graphiteReporter = null;

    /**
     * The reporters which a graph can start from its configuration.
     */
    public enum GraphReporter {
        CONSOLE, CSV, JMX, SLF4J, GRAPHITE
    }

    //For each reporter which graphs started, the claims of the open graphs which use it. A reporter which runs while no
    //graph holds a claim on it was started by other code, which stops it. The reporter's remove method ends the claims
    //on it, so that a claim which outlives its reporter can't stop one started later
    private final Map<GraphReporter, Set<Object>> graphReporterClaims = new EnumMap<>(GraphReporter.class);

    /**
     * Return the JanusGraph Metrics registry.
     *
     * @return the single {@code MetricRegistry} used for all of JanusGraph's Metrics
     *         monitoring
     */
    public MetricRegistry getRegistry() {
        return registry;
    }

    /**
     * Create a {@link ConsoleReporter} attached to the JanusGraph Metrics registry.
     *
     * @param reportInterval
     *            time to wait between dumping metrics to the console
     */
    public synchronized void addConsoleReporter(Duration reportInterval) {
        if (null != consoleReporter) {
            log.debug("Metrics ConsoleReporter already active; not creating another");
            return;
        }

        consoleReporter = ConsoleReporter.forRegistry(getRegistry()).build();
        consoleReporter.start(reportInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop a {@link ConsoleReporter} previously created by a call to
     * {@link #addConsoleReporter(Duration)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeConsoleReporter() {
        if (null != consoleReporter)
            consoleReporter.stop();

        consoleReporter = null;
        graphReporterClaims.remove(GraphReporter.CONSOLE);
    }

    /**
     * Create a {@link CsvReporter} attached to the JanusGraph Metrics registry.
     * <p>
     * The {@code output} argument must be non-null but need not exist. If it
     * doesn't already exist, this method attempts to create it by calling
     * {@link File#mkdirs()}.
     *
     * @param reportInterval
     *            time to wait between dumping metrics to CSV files in
     *            the configured directory
     * @param output
     *            the path to a directory into which Metrics will periodically
     *            write CSV data
     */
    public synchronized void addCsvReporter(Duration reportInterval,
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
        csvReporter.start(reportInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop a {@link CsvReporter} previously created by a call to
     * {@link #addCsvReporter(Duration, String)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeCsvReporter() {
        if (null != csvReporter)
            csvReporter.stop();

        csvReporter = null;
        graphReporterClaims.remove(GraphReporter.CSV);
    }

    /**
     * Create a {@link JmxReporter} attached to the JanusGraph Metrics registry.
     * <p>
     * If {@code domain} or {@code agentId} is null, then Metrics's uses its own
     * internal default value(s).
     * <p>
     * If {@code agentId} is non-null, then
     * {@link MBeanServerFactory#findMBeanServer(String agentId)} must return exactly
     * one {@code MBeanServer}. The reporter will register with that server. If
     * the {@code findMBeanServer(String agentId)} call returns no or multiple servers,
     * then this method logs an error and falls back on the Metrics default for
     * {@code agentId}.
     *
     * @param domain
     *            the JMX domain in which to continuously expose metrics
     * @param agentId
     *            the JMX agent ID
     */
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
            List<MBeanServer> servers = MBeanServerFactory.findMBeanServer(agentId);
            if (null != servers && 1 == servers.size()) {
                b.registerWith(servers.get(0));
            } else {
                log.error("Metrics Slf4jReporter agentId {} does not resolve to a single MBeanServer", agentId);
            }
        }

        jmxReporter = b.build();
        jmxReporter.start();
    }

    /**
     * Stop a {@link JmxReporter} previously created by a call to
     * {@link #addJmxReporter(String, String)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeJmxReporter() {
        if (null != jmxReporter)
            jmxReporter.stop();

        jmxReporter = null;
        graphReporterClaims.remove(GraphReporter.JMX);
    }

    /**
     * Create a {@link Slf4jReporter} attached to the JanusGraph Metrics registry.
     * <p>
     * If {@code loggerName} is null, or if it is non-null but
     * {@link LoggerFactory#getLogger(Class loggerName)} returns null, then Metrics's
     * default Slf4j logger name is used instead.
     *
     * @param reportInterval
     *            time to wait between writing metrics to the Slf4j
     *            logger
     * @param loggerName
     *            the name of the Slf4j logger that receives metrics
     */
    public synchronized void addSlf4jReporter(Duration reportInterval, String loggerName) {
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
        slf4jReporter.start(reportInterval.toMillis(), TimeUnit.MILLISECONDS);
    }

    /**
     * Stop a {@link Slf4jReporter} previously created by a call to
     * {@link #addSlf4jReporter(Duration, String)} and release it for GC. Idempotent
     * between calls to the associated add method. Does nothing before the first
     * call to the associated add method.
     */
    public synchronized void removeSlf4jReporter() {
        if (null != slf4jReporter)
            slf4jReporter.stop();

        slf4jReporter = null;
        graphReporterClaims.remove(GraphReporter.SLF4J);
    }

    /**
     * Create a {@link GraphiteReporter} attached to the JanusGraph Metrics registry.
     * <p>
     * If {@code prefix} is null, then Metrics's internal default prefix is used
     * (empty string at the time this comment was written).
     *
     * @param host
     *            the host to which Graphite reports are sent
     * @param port
     *            the port to which Graphite reports are sent
     * @param prefix
     *            the optional metrics prefix
     * @param reportInterval
     *            time to wait between sending metrics to the configured
     *            Graphite host and port
     */
    public synchronized void addGraphiteReporter(String host, int port,
            String prefix, Duration reportInterval) {

        Preconditions.checkNotNull(host);

        if (null != graphiteReporter) {
            log.debug("Metrics GraphiteReporter already active; not creating another");
            return;
        }

        Graphite graphite = new Graphite(new InetSocketAddress(host, port));

        GraphiteReporter.Builder b = GraphiteReporter
                .forRegistry(getRegistry());

        if (null != prefix)
            b.prefixedWith(prefix);

        b.filter(MetricFilter.ALL);

        graphiteReporter = b.build(graphite);
        graphiteReporter.start(reportInterval.toMillis(), TimeUnit.MILLISECONDS);
        log.info("Configured Graphite reporter host={} interval={} port={} prefix={}",
            host, reportInterval, port, prefix);
    }

    /**
     * Stop a {@link GraphiteReporter} previously created by a call to
     * {@link #addGraphiteReporter(String, int, String, Duration)} and release it
     * for GC. Idempotent between calls to the associated add method. Does
     * nothing before the first call to the associated add method.
     */
    public synchronized void removeGraphiteReporter() {
        if (null != graphiteReporter)
            graphiteReporter.stop();

        graphiteReporter = null;
        graphReporterClaims.remove(GraphReporter.GRAPHITE);
    }

    /**
     * Remove all JanusGraph Metrics reporters previously configured through the
     * {@code add*} methods on this class.
     */
    public synchronized void removeAllReporters() {
        removeConsoleReporter();
        removeCsvReporter();
        removeJmxReporter();
        removeSlf4jReporter();
        removeGraphiteReporter();
    }

    /**
     * Lets a graph use a reporter which its configuration asks for. Unless a reporter of the kind is running, it is
     * started, and the graph gets a claim on a reporter which graphs started, which it gives back with
     * {@link #releaseGraphReporter(GraphReporter, Object)}: the reporter stops once no claim on it is left. A reporter
     * which runs while no graph holds a claim on it was started by other code, which stops it: the graph uses it
     * without a claim.
     *
     * @param reporter the kind of reporter
     * @param start starts the reporter, by calling the {@code add*} method of its kind
     * @return the graph's claim on the reporter, or null for a reporter which other code started
     */
    public Object addGraphReporter(GraphReporter reporter, Runnable start) {
        Runnable stopFailed = null;
        Throwable failure = null;
        synchronized (this) {
            Set<Object> claims = graphReporterClaims.get(reporter);
            if (claims == null) {
                if (isRunning(reporter)) {
                    log.debug("A graph uses the running {} reporter, which other code started", reporter);
                    return null;
                }
                try {
                    start.run();
                    claims = new HashSet<>();
                    graphReporterClaims.put(reporter, claims);
                } catch (RuntimeException | Error e) {
                    //A reporter which began running before the failure would run without a claim: forget it here, and
                    //stop it as releaseGraphReporter() does, the JMX reporter under the lock, a periodic one outside
                    final Runnable stop = detach(reporter);
                    if (reporter == GraphReporter.JMX) {
                        stopAfterFailedStart(stop, e);
                        stopFailed = null;
                    } else {
                        stopFailed = stop;
                    }
                    failure = e;
                    claims = null;
                }
            } else {
                log.debug("A graph joins the running {} reporter, whose settings stay as they are", reporter);
            }
            if (claims != null) {
                final Object claim = new Object();
                claims.add(claim);
                return claim;
            }
        }
        if (stopFailed != null) {
            stopAfterFailedStart(stopFailed, failure);
        }
        if (failure instanceof Error) {
            throw (Error) failure;
        }
        throw (RuntimeException) failure;
    }

    //Stops a reporter whose start failed. The caller throws the start's failure; a failure of the stop goes with it
    private static void stopAfterFailedStart(Runnable stop, Throwable startFailure) {
        try {
            stop.run();
        } catch (RuntimeException | Error e) {
            startFailure.addSuppressed(e);
        }
    }

    /**
     * Gives back a claim which {@link #addGraphReporter(GraphReporter, Runnable)} returned, and stops the reporter
     * once no claim on it is left. A reporter which reports periodically reports one last time as it stops, on the
     * calling thread but without holding this manager's lock. Does nothing for a claim which the reporter's
     * {@code remove*} method has ended.
     *
     * @param reporter the kind of reporter
     * @param claim the claim
     */
    public void releaseGraphReporter(GraphReporter reporter, Object claim) {
        final Runnable stop;
        synchronized (this) {
            final Set<Object> claims = graphReporterClaims.get(reporter);
            if (claims == null || !claims.remove(claim) || !claims.isEmpty()) {
                return;
            }
            graphReporterClaims.remove(reporter);
            stop = detach(reporter);
            //The JMX reporter's stop only unregisters its MBeans, and a reporter started meanwhile would register the
            //same names, so it stops under the lock, as removeJmxReporter() stops it; the periodic reporters report once
            //more as they stop, which may block, so they stop outside
            if (reporter == GraphReporter.JMX) {
                stop.run();
                return;
            }
        }
        stop.run();
    }

    /**
     * Gives back the claims of a graph, each on its own: a reporter whose stop fails with an exception is logged, and
     * the others are still released; one whose stop fails with an error is logged too, and the error is thrown once
     * the others have been released.
     *
     * @param claims the claims of the graph, by the kind of their reporter
     */
    public void releaseGraphReporters(Map<GraphReporter, Object> claims) {
        Error error = null;
        for (Map.Entry<GraphReporter, Object> claim : claims.entrySet()) {
            try {
                releaseGraphReporter(claim.getKey(), claim.getValue());
            } catch (RuntimeException e) {
                log.warn("Could not stop the Metrics {} reporter", claim.getKey(), e);
            } catch (Error e) {
                log.warn("Could not stop the Metrics {} reporter", claim.getKey(), e);
                if (error == null) {
                    error = e;
                } else {
                    error.addSuppressed(e);
                }
            }
        }
        if (error != null) {
            throw error;
        }
    }

    //Forgets the running reporter of the given kind, and returns what stops it
    private Runnable detach(GraphReporter reporter) {
        final Runnable stop;
        switch (reporter) {
            case CONSOLE:
                stop = consoleReporter == null ? null : consoleReporter::stop;
                consoleReporter = null;
                break;
            case CSV:
                stop = csvReporter == null ? null : csvReporter::stop;
                csvReporter = null;
                break;
            case JMX:
                stop = jmxReporter == null ? null : jmxReporter::stop;
                jmxReporter = null;
                break;
            case SLF4J:
                stop = slf4jReporter == null ? null : slf4jReporter::stop;
                slf4jReporter = null;
                break;
            case GRAPHITE:
                stop = graphiteReporter == null ? null : graphiteReporter::stop;
                graphiteReporter = null;
                break;
            default:
                throw new AssertionError(reporter);
        }
        return stop == null ? () -> { } : stop;
    }

    /**
     * Whether a reporter of the given kind runs, for tests and tooling
     */
    public synchronized boolean isRunning(GraphReporter reporter) {
        switch (reporter) {
            case CONSOLE: return null != consoleReporter;
            case CSV: return null != csvReporter;
            case JMX: return null != jmxReporter;
            case SLF4J: return null != slf4jReporter;
            case GRAPHITE: return null != graphiteReporter;
            default: throw new AssertionError(reporter);
        }
    }

    public Counter getCounter(String name) {
        return getRegistry().counter(name);
    }

    public Counter getCounter(String prefix, String... names) {
        return getRegistry().counter(MetricRegistry.name(prefix, names));
    }

    /**
     * Returns the timer registered under {@code name}, creating it on first use.
     * <p>
     * A timer created through {@link MetricRegistry#timer(String)} samples its durations into an
     * {@link com.codahale.metrics.ExponentiallyDecayingReservoir}, which takes the read side of a
     * {@code ReentrantReadWriteLock} on every update. Every acquisition and release of that lock is a
     * compare-and-set on one shared word, so the many threads that update the same few timers -
     * {@code stores.getSlice.time}, {@code stores.mutate.time}, {@code stores.acquireLock.time} -
     * spin against each other instead of doing storage work. The timers this class hands out use
     * {@link LockFreeExponentiallyDecayingReservoir} instead: the same forward-decay sampling with the
     * same defaults, but the reservoir state is swapped atomically rather than guarded by a lock.
     */
    public Timer getTimer(String name) {
        return getRegistry().timer(name, TIMER_SUPPLIER);
    }

    public Timer getTimer(String prefix, String... names) {
        return getTimer(MetricRegistry.name(prefix, names));
    }

    /**
     * Returns the histogram registered under {@code name}, creating it on first use. See
     * {@link #getTimer(String)} for why the histogram samples into a lock-free reservoir.
     */
    public Histogram getHistogram(String name) {
        return getRegistry().histogram(name, HISTOGRAM_SUPPLIER);
    }

    public Histogram getHistogram(String prefix, String... names) {
        return getHistogram(MetricRegistry.name(prefix, names));
    }

    public boolean remove(String name) {
        return getRegistry().remove(name);
    }
}
