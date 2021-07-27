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
import java.util.List;
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



    private final MetricRegistry registry     = new MetricRegistry();
    private ConsoleReporter consoleReporter   = null;
    private CsvReporter csvReporter           = null;
    private JmxReporter jmxReporter           = null;
    private Slf4jReporter slf4jReporter       = null;
    private GraphiteReporter graphiteReporter = null;

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

    public Counter getCounter(String name) {
        return getRegistry().counter(name);
    }

    public Counter getCounter(String prefix, String... names) {
        return getRegistry().counter(MetricRegistry.name(prefix, names));
    }

    public Timer getTimer(String name) {
        return getRegistry().timer(name);
    }

    public Timer getTimer(String prefix, String... names) {
        return getRegistry().timer(MetricRegistry.name(prefix, names));
    }

    public Histogram getHistogram(String name) {
        return getRegistry().histogram(name);
    }

    public Histogram getHistogram(String prefix, String... names) {
        return getRegistry().histogram(MetricRegistry.name(prefix, names));
    }

    public boolean remove(String name) {
        return getRegistry().remove(name);
    }
}
