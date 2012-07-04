package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
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

    public void write(final NullWritable nullKey, final FaunusVertex vertex) throws IOException {
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

            List<Edge> edges = (List<Edge>) vertex.getEdges(Direction.OUT);
            if (!edges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : edges) {
                    final JSONObject edge = new JSONObject();
                    edge.put(JSONTokens.IN_ID, outEdge.getVertex(Direction.IN).getId());
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

            edges = (List<Edge>) vertex.getEdges(Direction.IN);
            if (!edges.isEmpty()) {
                final JSONArray inEdgesArray = new JSONArray();
                for (final Edge inEdge : edges) {
                    final JSONObject edge = new JSONObject();
                    edge.put(JSONTokens.OUT_ID, inEdge.getVertex(Direction.OUT).getId());
                    edge.put(JSONTokens.LABEL, inEdge.getLabel());
                    final Set<String> edgeKeys = inEdge.getPropertyKeys();
                    if (!edgeKeys.isEmpty()) {
                        final JSONObject edgeProperties = new JSONObject();
                        for (final String edgeKey : edgeKeys) {
                            edgeProperties.put(edgeKey, inEdge.getProperty(edgeKey));
                        }
                        edge.put(JSONTokens.PROPERTIES, edgeProperties);
                    }
                    inEdgesArray.add(edge);
                }
                object.put(JSONTokens.IN_E, inEdgesArray);
            }
            this.out.write(object.toString().getBytes(UTF8));
            this.out.write(NEWLINE);
        }
    }

    public synchronized void close(TaskAttemptContext context) throws IOException {
        out.close();
    }
}
