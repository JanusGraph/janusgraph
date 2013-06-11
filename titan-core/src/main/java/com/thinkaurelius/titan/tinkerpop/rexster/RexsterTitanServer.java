package com.thinkaurelius.titan.tinkerpop.rexster;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.rexster.Tokens;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.*;
import com.tinkerpop.rexster.server.metrics.ReporterConfig;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.HierarchicalConfiguration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Standalone Titan database with fronting Rexster server.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class RexsterTitanServer {

    private static final Logger log =
            LoggerFactory.getLogger(RexsterTitanServer.class);

    public static final String DEFAULT_GRAPH_NAME = "graph";
    public static final String REXSTER_CONFIG_NS = "rexster";


    public static final String SHUTDOWN_HOST_KEY = "shutdown-host";
    public static final String SHUTDOWN_HOST_VALUE = "0.0.0.0";

    public static final String SHUTDOWN_PORT_KEY = "shutdown-port";
    public static final int SHUTDOWN_PORT_VALUE = RexsterSettings.DEFAULT_SHUTDOWN_PORT;

    private final Configuration titanConfig;
    private final XMLConfiguration rexsterConfig;

    private RexProRexsterServer rexProServer = null;
    private HttpRexsterServer httpServer = null;
    private TitanGraph graph;

    public RexsterTitanServer(final XMLConfiguration rexsterConfig, final Configuration titanConfig) {
        Preconditions.checkNotNull(rexsterConfig);
        Preconditions.checkNotNull(titanConfig);

        this.rexsterConfig = rexsterConfig;
        this.titanConfig = titanConfig;
    }

    public void start() {
        graph = TitanFactory.open(titanConfig);
        
        final List<HierarchicalConfiguration> extensionConfigurations = ((XMLConfiguration) rexsterConfig).configurationsAt(Tokens.REXSTER_GRAPH_EXTENSIONS_PATH);
        log.info("Extension Config: " + extensionConfigurations.toString());
        final RexsterApplication ra = new TitanRexsterApplication(DEFAULT_GRAPH_NAME, graph, extensionConfigurations);

        final RexsterProperties properties = new RexsterProperties(rexsterConfig);
        configureScriptEngine(properties);

        final ReporterConfig reporterConfig = new ReporterConfig(properties, ra.getMetricRegistry());
        this.rexsterConfig.addProperty("http-reporter-enabled", reporterConfig.isHttpReporterEnabled());
        this.rexsterConfig.addProperty("http-reporter-duration", reporterConfig.getDurationTimeUnitConversion());
        this.rexsterConfig.addProperty("http-reporter-convert", reporterConfig.getRateTimeUnitConversion());
        reporterConfig.enable();

        final boolean isRexProConfigured = rexsterConfig.subset("rexpro").getKeys().hasNext();
        final boolean isHttpConfigured = rexsterConfig.subset("http").getKeys().hasNext();
        
        if (isRexProConfigured || !isHttpConfigured) {
            rexProServer = new RexProRexsterServer(rexsterConfig);
            startRexProServer(ra);
        }

        if (isHttpConfigured || !isRexProConfigured) {
            // turn off dog house...always
            this.rexsterConfig.setProperty("http.enable-doghouse", false);
            httpServer = new HttpRexsterServer(rexsterConfig);
            startHttpServer(ra);
        }
    }

    private void configureScriptEngine(final RexsterProperties properties) {
        // the EngineController needs to be configured statically before requests start serving so that it can
        // properly construct ScriptEngine objects with the correct reset policy.
        final int scriptEngineThreshold = properties.getScriptEngineResetThreshold();
        final String scriptEngineInitFile = properties.getScriptEngineInitFile();

        // allow scriptengines to be configured so that folks can drop in different gremlin flavors.
        final List configuredScriptEngineNames = properties.getConfiguredScriptEngines();
        if (configuredScriptEngineNames == null || configuredScriptEngineNames.isEmpty()) {
            // configure to default with gremlin-groovy
            log.info("No configuration for <script-engines>.  Using gremlin-groovy by default.");
            EngineController.configure(scriptEngineThreshold, scriptEngineInitFile);
        } else {
            EngineController.configure(scriptEngineThreshold, scriptEngineInitFile, new HashSet<String>(configuredScriptEngineNames));
        }

        log.info(String.format(
                "Gremlin ScriptEngine configured to reset every [%s] requests. Set to -1 to never reset.",
                scriptEngineThreshold));
    }

    public void startDaemon() {
        start();
        final ShutdownManager shutdownManager = new ShutdownManager(rexsterConfig.getString(SHUTDOWN_HOST_KEY, SHUTDOWN_HOST_VALUE), rexsterConfig.getInt(SHUTDOWN_PORT_KEY, SHUTDOWN_PORT_VALUE));
        shutdownManager.registerShutdownListener(new ShutdownManager.ShutdownListener() {
            public void shutdown() {
                stop();
            }
        });
        try {
            shutdownManager.start();
        } catch (Exception e) {
            throw new IllegalStateException("Could not register shutdown manager", e);
        }
        shutdownManager.waitForShutdown();
    }

    public void stop() {
        // shutdown grizzly/graphs
        try {
            if (rexProServer != null) rexProServer.stop();
            if (httpServer != null) httpServer.stop();
        } catch (Exception ex) {
            log.error("Exception when shutting down rexster: {}", ex);
        }
        graph.shutdown();
    }

    public static void main(String[] args) throws Exception {
        RexsterTitanServer server;
        if (args.length == 2) {
            final Configuration titanConfig = new PropertiesConfiguration(args[1]);
            final XMLConfiguration rexsterConfig = new XMLConfiguration(args[0]);
            server = new RexsterTitanServer(rexsterConfig, titanConfig);
        } else if (args.length == 1) {
            final Configuration config = new PropertiesConfiguration(args[0]);
            server = new RexsterTitanServer(convert2RexproConfiguration(config.subset(REXSTER_CONFIG_NS)), config);
        } else {
            throw new IllegalArgumentException("Expected at least one configuration file");
        }
        server.startDaemon();
    }

    public static XMLConfiguration convert2RexproConfiguration(final Configuration config) {
        final XMLConfiguration rexsterConfig = new XMLConfiguration();
        final Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            log.debug("Setting Rexster RexPro Config Option:{}={}", key, config.getProperty(key));
            rexsterConfig.addProperty(key, config.getProperty(key));
        }
        return rexsterConfig;
    }

    private void startRexProServer(RexsterApplication ra) {
        try {
            if (rexProServer != null) {
                rexProServer.start(ra);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not start RexPro Server", e);
        }
    }

    private void startHttpServer(RexsterApplication ra) {
        try {
            if (httpServer != null) {
                httpServer.start(ra);
            }
        } catch (Exception e) {
            throw new IllegalStateException("Could not start HTTP Server", e);
        }
    }
}
