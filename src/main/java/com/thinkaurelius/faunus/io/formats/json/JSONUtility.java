package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
public class JSONUtility {

    private static final FaunusElementFactory elementFactory = new FaunusElementFactory();

    public static List<FaunusVertex> fromJSON(final InputStream in) throws IOException {
        final List<FaunusVertex> vertices = new LinkedList<FaunusVertex>();
        final BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = bfs.readLine()) != null) {
            vertices.add(JSONUtility.fromJSON(line));
        }
        bfs.close();
        return vertices;

    }

    public static FaunusVertex fromJSON(final String line) throws IOException {
        try {
            final JSONObject json = new JSONObject(new JSONTokener(line));

            final Set<String> ignore = new HashSet<String>();
            ignore.add(JSONTokens.OUT_E);
            ignore.add(JSONTokens.IN_E);
            final FaunusVertex vertex = (FaunusVertex) GraphSONUtility.vertexFromJson(json, elementFactory, false, ignore);

            final JSONArray outEdges = json.optJSONArray(JSONTokens.OUT_E);
            writeEdge(vertex, outEdges, OUT);

            final JSONArray inEdges = (JSONArray) json.optJSONArray(JSONTokens.IN_E);
            writeEdge(vertex, inEdges, IN);

            return vertex;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private static void writeEdge(final FaunusVertex vertex, final JSONArray edges,
                                  final Direction direction) throws JSONException, IOException {
        if (null != edges) {
            for (int ix = 0; ix < edges.length(); ix++) {
                final JSONObject edge = edges.optJSONObject(ix);

                FaunusEdge faunusEdge = null;
                if (direction == Direction.IN) {
                    final long outVertexId = edge.optLong(GraphSONTokens._OUT_V);
                    faunusEdge = (FaunusEdge) GraphSONUtility.edgeFromJSON(edge, new FaunusVertex(outVertexId), vertex, elementFactory, false, null);
                } else if (direction == Direction.OUT) {
                    final long inVertexId = edge.optLong(GraphSONTokens._IN_V);
                    faunusEdge = (FaunusEdge) GraphSONUtility.edgeFromJSON(edge, vertex, new FaunusVertex(inVertexId), elementFactory, false, null);
                }

                if (faunusEdge != null) {
                    vertex.addEdge(direction, faunusEdge);
                }
            }
        }
    }

    public static JSONObject toJSON(final Vertex vertex) throws IOException {
        try {
            final JSONObject object = GraphSONUtility.jsonFromElement(vertex);

            List<Edge> edges = (List<Edge>) vertex.getEdges(OUT);
            if (!edges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : edges) {
                    outEdgesArray.put(GraphSONUtility.jsonFromElement(outEdge));
                }
                object.put(JSONTokens.OUT_E, outEdgesArray);
            }

            edges = (List<Edge>) vertex.getEdges(IN);
            if (!edges.isEmpty()) {
                final JSONArray inEdgesArray = new JSONArray();
                for (final Edge inEdge : edges) {
                    inEdgesArray.put(GraphSONUtility.jsonFromElement(inEdge));
                }
                object.put(JSONTokens.IN_E, inEdgesArray);
            }

            return object;
        } catch (JSONException jex) {
            throw new IOException(jex);
        }
    }
}
