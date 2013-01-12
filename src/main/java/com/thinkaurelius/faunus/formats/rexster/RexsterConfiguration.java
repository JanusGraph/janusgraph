package com.thinkaurelius.faunus.formats.rexster;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterConfiguration {
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_HOSTNAME = "faunus.graph.input.rexster.hostname";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_PORT = "faunus.graph.input.rexster.port";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_SSL = "faunus.graph.input.rexster.ssl";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_GRAPH = "faunus.graph.input.rexster.graph";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_V_ESTIMATE = "faunus.graph.input.rexster.v-estimate";

    private Configuration conf;

    public RexsterConfiguration(final Configuration job) {
        this.conf = job;
    }

    public Configuration getConf() {
        return conf;
    }

    public boolean getSsl() {
        return this.conf.getBoolean(FAUNUS_GRAPH_INPUT_REXSTER_SSL, false);
    }

    public String getRestAddress() {
        return this.conf.get(FAUNUS_GRAPH_INPUT_REXSTER_HOSTNAME);
    }

    public int getRestPort() {
        return this.conf.getInt(FAUNUS_GRAPH_INPUT_REXSTER_PORT, 8182);
    }

    public String getGraph() {
        return this.conf.get(FAUNUS_GRAPH_INPUT_REXSTER_GRAPH);
    }

    public int getEstimatedVertexCount() {
        return this.conf.getInt(FAUNUS_GRAPH_INPUT_REXSTER_V_ESTIMATE, 10000);
    }

    public String getHttpProtocol() {
        return this.getSsl() ? "https" : "http";
    }

    public String getRestEndpoint() {
        return String.format("%s://%s:%s/graphs/%s/tp/gremlin",
                this.getHttpProtocol(), this.getRestAddress(),
                this.getRestPort(), this.getGraph());
    }

    public String getRestStreamEndpoint() {
        return String.format("%s://%s:%s/graphs/%s/%s/%s",
                this.getHttpProtocol(), this.getRestAddress(),
                this.getRestPort(), this.getGraph(), FaunusRexsterExtension.EXTENSION_NAMESPACE,
                FaunusRexsterExtension.EXTENSION_NAME);
    }
}
