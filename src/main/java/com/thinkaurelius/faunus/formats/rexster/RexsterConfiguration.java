package com.thinkaurelius.faunus.formats.rexster;

import org.apache.hadoop.conf.Configuration;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterConfiguration {
    public static final String REXSTER_INPUT_REST_ADDRESS = "rexster.input.rest.address";
    public static final String REXSTER_INPUT_REST_PORT = "rexster.input.rest.port";
    public static final String REXSTER_INPUT_GRAPH = "rexster.input.graph";
    public static final String REXSTER_INPUT_V_ESTIMATE = "rexster.input.v.estimate";

    private Configuration conf;

    public RexsterConfiguration(final Configuration job) {
        this.conf = job;
    }

    public Configuration getConf() {
        return conf;
    }

    public String getRestAddress() {
        return this.conf.get(REXSTER_INPUT_REST_ADDRESS);
    }

    public int getRestPort(){
        return this.conf.getInt(REXSTER_INPUT_REST_PORT, 8182);
    }

    public String getGraph() {
        return this.conf.get(REXSTER_INPUT_GRAPH);
    }

    public int getEstimatedVertexCount() {
        return this.conf.getInt(REXSTER_INPUT_V_ESTIMATE, 10000);
    }

    public String getEndpoint() {
        return String.format("http://%s:%s/graphs/%s/tp/gremlin", this.getRestAddress(),
                this.getRestPort(), this.getGraph());
    }
}
