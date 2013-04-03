package com.thinkaurelius.faunus.formats.rexster;

import com.thinkaurelius.faunus.formats.rexster.util.HttpHelper;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;

/**
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class RexsterConfiguration {

    public static final int VERTEX_TRUE_COUNT = -1;

    public static final String FAUNUS_GRAPH_INPUT_REXSTER_HOSTNAME = "faunus.graph.input.rexster.hostname";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_PORT = "faunus.graph.input.rexster.port";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_SSL = "faunus.graph.input.rexster.ssl";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_GRAPH = "faunus.graph.input.rexster.graph";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_V_ESTIMATE = "faunus.graph.input.rexster.v-estimate";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_USERNAME = "faunus.graph.input.rexster.username";
    public static final String FAUNUS_GRAPH_INPUT_REXSTER_PASSWORD = "faunus.graph.input.rexster.password";

    private Configuration conf;

    private static long trueVertexCount = Long.MIN_VALUE;

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

    public String getAuthenticationHeaderValue() {
        return "Basic " + Base64.encodeBase64URLSafeString(
                (this.conf.get(FAUNUS_GRAPH_INPUT_REXSTER_USERNAME, "") + ":" + this.conf.get(FAUNUS_GRAPH_INPUT_REXSTER_USERNAME, ""))
                        .getBytes());
    }

    public synchronized long getEstimatedVertexCount() {
        long vertexCount = this.conf.getInt(FAUNUS_GRAPH_INPUT_REXSTER_V_ESTIMATE, 10000);

        // getting the true count means doing a g.V over in rexster.  this method is synchronized so that
        // g.V.count() is not called more than once for the job.
        if (vertexCount == VERTEX_TRUE_COUNT) {
            if (trueVertexCount == Long.MIN_VALUE) {
                trueVertexCount = getTrueVertexCount();
            }

            vertexCount = trueVertexCount;
        }

        return vertexCount;
    }

    public String getHttpProtocol() {
        return this.getSsl() ? "https" : "http";
    }

    public String getRestEndpoint() {
        return String.format("%s://%s:%s/graphs/%s/%s/%s",
                this.getHttpProtocol(), this.getRestAddress(),
                this.getRestPort(), this.getGraph(), FaunusRexsterExtension.EXTENSION_NAMESPACE,
                FaunusRexsterExtension.EXTENSION_NAME);
    }

    public String getRestStreamEndpoint() {
        return String.format("%s/%s",
                this.getRestEndpoint(), FaunusRexsterExtension.EXTENSION_METHOD_STREAM);
    }

    public String getRestCountEndpoint() {
        return String.format("%s/%s",
                this.getRestEndpoint(), FaunusRexsterExtension.EXTENSION_METHOD_COUNT);
    }

    private long getTrueVertexCount() {
        try {
            final HttpURLConnection connection = HttpHelper.createConnection(
                    this.getRestCountEndpoint(), this.getAuthenticationHeaderValue());
            final JSONObject json = new JSONObject(convertStreamToString(connection.getInputStream()));

            return json.optLong(FaunusRexsterExtension.EXTENSION_METHOD_COUNT);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    private static String convertStreamToString(final InputStream is) throws Exception {
        final BufferedReader reader = new BufferedReader(new InputStreamReader(is));
        final StringBuilder sb = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            sb.append(line + "\n");
        }
        is.close();
        return sb.toString();
    }
}
