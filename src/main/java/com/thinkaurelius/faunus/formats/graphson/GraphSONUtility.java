package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Graph;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONUtility {

    private static final String _OUT_E = "_outE";
    private static final String _IN_E = "_inE";

    private static final FaunusElementFactory elementFactory = new FaunusElementFactory();

    public static List<FaunusVertex> fromJSON(final InputStream in) throws IOException {
        final List<FaunusVertex> vertices = new LinkedList<FaunusVertex>();
        final BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = bfs.readLine()) != null) {
            vertices.add(GraphSONUtility.fromJSON(line));
        }
        bfs.close();
        return vertices;

    }

    public static FaunusVertex fromJSON(final String line) throws IOException {
        try {
            final JSONObject json = new JSONObject(new JSONTokener(line));

            final Set<String> ignore = new HashSet<String>();
            ignore.add(_OUT_E);
            ignore.add(_IN_E);
            ignore.add(GraphSONTokens._TYPE);
            final FaunusVertex vertex = (FaunusVertex) com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility.vertexFromJson(json, elementFactory, false, ignore);

            fromJSONEdges(vertex, json.optJSONArray(_OUT_E), OUT);
            fromJSONEdges(vertex, json.optJSONArray(_IN_E), IN);

            return vertex;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private static void fromJSONEdges(final FaunusVertex vertex, final JSONArray edges, final Direction direction) throws JSONException, IOException {
        if (null != edges) {

            final Set<String> ignore = new HashSet<String>();
            ignore.add(GraphSONTokens._TYPE);

            for (int ix = 0; ix < edges.length(); ix++) {
                final JSONObject edge = edges.optJSONObject(ix);
                FaunusEdge faunusEdge = null;
                if (direction == Direction.IN) {
                    final long outVertexId = edge.optLong(GraphSONTokens._OUT_V);
                    ignore.add(GraphSONTokens._OUT_V);
                    faunusEdge = (FaunusEdge) com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility.edgeFromJSON(edge, new FaunusVertex(outVertexId), vertex, elementFactory, false, ignore);
                    ignore.remove(GraphSONTokens._OUT_V);
                } else if (direction == Direction.OUT) {
                    final long inVertexId = edge.optLong(GraphSONTokens._IN_V);
                    ignore.add(GraphSONTokens._IN_V);
                    faunusEdge = (FaunusEdge) com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility.edgeFromJSON(edge, vertex, new FaunusVertex(inVertexId), elementFactory, false, ignore);
                    ignore.remove(GraphSONTokens._IN_V);
                }

                if (faunusEdge != null) {
                    vertex.addEdge(direction, faunusEdge);
                }
            }
        }
    }

    public static JSONObject toJSON(final Vertex vertex) throws IOException {
        try {
            final JSONObject object = com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility.jsonFromElement(vertex);
            object.remove(GraphSONTokens._TYPE);
            object.put(GraphSONTokens._ID, Long.valueOf(object.remove(GraphSONTokens._ID).toString()));

            List<Edge> edges = (List<Edge>) vertex.getEdges(OUT);
            if (!edges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : edges) {
                    final JSONObject edgeObject = com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility.jsonFromElement(outEdge);
                    edgeObject.put(GraphSONTokens._ID, Long.valueOf(edgeObject.remove(GraphSONTokens._ID).toString()));
                    edgeObject.remove(GraphSONTokens._TYPE);
                    edgeObject.remove(GraphSONTokens._OUT_V);
                    edgeObject.put(GraphSONTokens._IN_V, Long.valueOf(edgeObject.remove(GraphSONTokens._IN_V).toString()));
                    outEdgesArray.put(edgeObject);
                }
                object.put(_OUT_E, outEdgesArray);
            }

            edges = (List<Edge>) vertex.getEdges(IN);
            if (!edges.isEmpty()) {
                final JSONArray inEdgesArray = new JSONArray();
                for (final Edge inEdge : edges) {
                    final JSONObject edgeObject = com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility.jsonFromElement(inEdge);
                    edgeObject.put(GraphSONTokens._ID, Long.valueOf(edgeObject.remove(GraphSONTokens._ID).toString()));
                    edgeObject.remove(GraphSONTokens._TYPE);
                    edgeObject.remove(GraphSONTokens._IN_V);
                    edgeObject.put(GraphSONTokens._OUT_V, Long.valueOf(edgeObject.remove(GraphSONTokens._OUT_V).toString()));
                    inEdgesArray.put(edgeObject);
                }
                object.put(_IN_E, inEdgesArray);
            }

            return object;
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    private static class FaunusElementFactory implements ElementFactory<FaunusVertex, FaunusEdge> {
        @Override
        public FaunusEdge createEdge(final Object id, final FaunusVertex out, final FaunusVertex in, final String label) {
            return new FaunusEdge(convertIdentifier(id), out.getIdAsLong(), in.getIdAsLong(), label);
        }

        @Override
        public FaunusVertex createVertex(Object id) {
            return new FaunusVertex(convertIdentifier(id));
        }

        private long convertIdentifier(Object id) {
            long identifier = -1l;
            if (id != null) {
                try {
                    identifier = Long.parseLong(id.toString());
                } catch (NumberFormatException nfe) {
                    identifier = -1l;
                }
            }
            return identifier;
        }
    }

    public static void generateGraphSON(final Graph graph, final OutputStream outputStream) throws IOException {
        final BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
        for (final Vertex vertex : graph.getVertices()) {
            bw.write(GraphSONUtility.toJSON(vertex) + "\n");
        }
        bw.close();
    }
}
