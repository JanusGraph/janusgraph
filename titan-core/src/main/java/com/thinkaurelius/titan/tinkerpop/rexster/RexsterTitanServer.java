package com.thinkaurelius.titan.tinkerpop.rexster;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanGraph;
import com.tinkerpop.rexster.protocol.EngineController;
import com.tinkerpop.rexster.server.*;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.configuration.XMLConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
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
    private final Configuration rexsterConfig;

    private RexProRexsterServer server;
    private TitanGraph graph;

    public RexsterTitanServer(final XMLConfiguration rexsterConfig, final Configuration titanConfig) {
        Preconditions.checkNotNull(rexsterConfig);
        Preconditions.checkNotNull(titanConfig);

        // can drop this check on release of Rexster 2.3.0
        if (!rexsterConfig.subset("security.authentication").getKeys().hasNext()) {
            rexsterConfig.addProperty("security.authentication.type", "none");
        }

        server = new RexProRexsterServer(rexsterConfig);
        this.rexsterConfig = rexsterConfig;
        this.titanConfig = titanConfig;
    }

    public void start() {
        EngineController.configure(-1, null);
        graph = TitanFactory.open(titanConfig);
        final RexsterApplication ra = new DefaultRexsterApplication(DEFAULT_GRAPH_NAME, graph);
        try {
            server.start(ra);
        } catch (Exception e) {
            throw new IllegalStateException("Could not start rexster application", e);
        }
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
            server.stop();
        } catch (Exception ex) {
            log.error("Exception when shutting down rexster: {}", ex);
        }
        graph.shutdown();
    }

    public static void main(String[] args) throws Exception {
        RexsterTitanServer server;
        if (args.length == 2) {
            Configuration titanConfig = new PropertiesConfiguration(args[1]);
            XMLConfiguration rexsterConfig = new XMLConfiguration(args[0]);
            server = new RexsterTitanServer(rexsterConfig, titanConfig);
        } else if (args.length == 1) {
            Configuration config = new PropertiesConfiguration(args[0]);
            server = new RexsterTitanServer(convert2RexproConfiguration(config.subset(REXSTER_CONFIG_NS)), config);
        } else {
            throw new IllegalArgumentException("Expected at least one configuration file");
        }
        server.startDaemon();
    }

    public static XMLConfiguration convert2RexproConfiguration(final Configuration config) {
        final XMLConfiguration rexsterConfig = new XMLConfiguration();
        Iterator<String> keys = config.getKeys();
        while (keys.hasNext()) {
            String key = keys.next();
            log.debug("Setting Rexster RexPro Config Option:{}={}", key, config.getProperty(key));
            rexsterConfig.addProperty(key, config.getProperty(key));
        }
        return rexsterConfig;
    }


}
