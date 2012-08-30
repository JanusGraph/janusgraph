package com.thinkaurelius.faunus.formats.rexster;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterConfiguration {
    public static final String REXSTER_INPUT_ADDRESS = "rexster.input.address";
    public static final String REXSTER_INPUT_PORT = "rexster.input.port";
    public static final String REXSTER_INPUT_SSL = "rexster.input.ssl";
    public static final String REXSTER_INPUT_GRAPH = "rexster.input.graph";
    public static final String REXSTER_INPUT_V_ESTIMATE = "rexster.input.v.estimate";

    private Configuration conf;

    public RexsterConfiguration(final Configuration job) {
        this.conf = job;
    }

    public Configuration getConf() {
        return conf;
    }

    public boolean getSsl() {
        return this.conf.getBoolean(REXSTER_INPUT_SSL, false);
    }

    public String getRestAddress() {
        return this.conf.get(REXSTER_INPUT_ADDRESS);
    }

    public int getRestPort() {
        return this.conf.getInt(REXSTER_INPUT_PORT, 8182);
    }

    public String getGraph() {
        return this.conf.get(REXSTER_INPUT_GRAPH);
    }

    public int getEstimatedVertexCount() {
        return this.conf.getInt(REXSTER_INPUT_V_ESTIMATE, 10000);
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
