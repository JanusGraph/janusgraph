package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.pgm.Edge;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusJSONRecordWriter extends RecordWriter<NullWritable, FaunusVertex> {
    private static final String UTF8 = "UTF-8";
    private static final byte[] NEWLINE;

    static {
        try {
            NEWLINE = "\n".getBytes(UTF8);
        } catch (UnsupportedEncodingException uee) {
            throw new IllegalArgumentException("can't find " + UTF8 + " encoding");
        }
    }

    protected DataOutputStream out;

    public FaunusJSONRecordWriter(final DataOutputStream out) {
        this.out = out;
    }

    public synchronized void write(final NullWritable nullKey, final FaunusVertex vertex) throws IOException {
        if (null != vertex) {
            final JSONObject object = new JSONObject();
            object.put(JSONTokens.ID, vertex.getId());
            final Set<String> vertexKeys = vertex.getPropertyKeys();
            if (!vertexKeys.isEmpty()) {
                final JSONObject vertexProperties = new JSONObject();
                for (String vertexKey : vertexKeys) {
                    vertexProperties.put(vertexKey, vertex.getProperty(vertexKey));
                }
                object.put(JSONTokens.PROPERTIES, vertexProperties);
            }

            final List<Edge> outEdges = (List<Edge>) vertex.getOutEdges();
            if (!outEdges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : outEdges) {
                    final JSONObject edge = new JSONObject();
                    edge.put(JSONTokens.IN_ID, outEdge.getInVertex().getId());
                    edge.put(JSONTokens.LABEL, outEdge.getLabel());
                    final Set<String> edgeKeys = outEdge.getPropertyKeys();
                    if (!edgeKeys.isEmpty()) {
                        final JSONObject edgeProperties = new JSONObject();
                        for (final String edgeKey : edgeKeys) {
                            edgeProperties.put(edgeKey, outEdge.getProperty(edgeKey));
                        }
                        edge.put(JSONTokens.PROPERTIES, edgeProperties);
                    }
                    outEdgesArray.add(edge);
                }
                object.put(JSONTokens.OUT_E, outEdgesArray);
            }
            this.out.write(object.toString().getBytes(UTF8));
            this.out.write(NEWLINE);
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
