package com.thinkaurelius.faunus.formats.graphson;

import com.thinkaurelius.faunus.FaunusEdge;
import com.thinkaurelius.faunus.FaunusVertex;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.ElementPropertyConfig;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.*;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public class GraphSONUtility {

    private static final String _OUT_E = "_outE";
    private static final String _IN_E = "_inE";
    private static final String EMPTY_STRING = "";

    private static final Set<String> VERTEX_IGNORE = new HashSet<String>(Arrays.asList(GraphSONTokens._TYPE, _OUT_E, _IN_E));
    private static final Set<String> EDGE_IGNORE = new HashSet<String>(Arrays.asList(GraphSONTokens._TYPE, GraphSONTokens._OUT_V, GraphSONTokens._IN_V));

    private static final FaunusElementFactory elementFactory = new FaunusElementFactory();

    private static final Map<Set<String>, com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility> graphsonVertexCache
            = new HashMap<Set<String>, com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility>();
    private static final Map<Set<String>, com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility> graphsonEdgeCache
            = new HashMap<Set<String>, com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility>();

    private static final com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility graphson
            = new com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility(GraphSONMode.COMPACT, elementFactory,
            ElementPropertyConfig.ExcludeProperties(VERTEX_IGNORE, EDGE_IGNORE));

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

    public static FaunusVertex fromJSON(String line) throws IOException {
        try {
            final JSONObject json = new JSONObject(new JSONTokener(line));
            line = EMPTY_STRING; // clear up some memory

            final FaunusVertex vertex = (FaunusVertex) graphson.vertexFromJson(json);

            fromJSONEdges(vertex, json.optJSONArray(_OUT_E), OUT);
            json.remove(_OUT_E); // clear up some memory
            fromJSONEdges(vertex, json.optJSONArray(_IN_E), IN);
            json.remove(_IN_E); // clear up some memory

            return vertex;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    private static void fromJSONEdges(final FaunusVertex vertex, final JSONArray edges, final Direction direction) throws JSONException, IOException {
        if (null != edges) {
            for (int i = 0; i < edges.length(); i++) {
                final JSONObject edge = edges.optJSONObject(i);
                FaunusEdge faunusEdge = null;
                if (direction.equals(Direction.IN)) {
                    faunusEdge = (FaunusEdge) graphson.edgeFromJson(edge, new FaunusVertex(edge.optLong(GraphSONTokens._OUT_V)), vertex);
                } else if (direction.equals(Direction.OUT)) {
                    faunusEdge = (FaunusEdge) graphson.edgeFromJson(edge, vertex, new FaunusVertex(edge.optLong(GraphSONTokens._IN_V)));
                }

                if (faunusEdge != null) {
                    vertex.addEdge(direction, faunusEdge);
                }
            }
        }
    }

    public static JSONObject toJSON(final Vertex vertex) throws IOException {
        try {
            final JSONObject object = getGraphSONUtility(vertex, false).jsonFromElement(vertex);

            // force the ID to long.  with blueprints, most implementations will send back a long, but
            // some like TinkerGraph will return a string.  the same is done for edges below
            object.put(GraphSONTokens._ID, Long.valueOf(object.remove(GraphSONTokens._ID).toString()));

            List<Edge> edges = (List<Edge>) vertex.getEdges(OUT);
            if (!edges.isEmpty()) {
                final JSONArray outEdgesArray = new JSONArray();
                for (final Edge outEdge : edges) {
                    final JSONObject edgeObject = getGraphSONUtility(outEdge, true).jsonFromElement(outEdge);
                    edgeObject.put(GraphSONTokens._ID, Long.valueOf(edgeObject.remove(GraphSONTokens._ID).toString()));
                    edgeObject.put(GraphSONTokens._IN_V, Long.valueOf(edgeObject.remove(GraphSONTokens._IN_V).toString()));
                    outEdgesArray.put(edgeObject);
                }
                object.put(_OUT_E, outEdgesArray);
            }

            edges = (List<Edge>) vertex.getEdges(IN);
            if (!edges.isEmpty()) {
                final JSONArray inEdgesArray = new JSONArray();
                for (final Edge inEdge : edges) {
                    final JSONObject edgeObject = getGraphSONUtility(inEdge, false).jsonFromElement(inEdge);
                    edgeObject.put(GraphSONTokens._ID, Long.valueOf(edgeObject.remove(GraphSONTokens._ID).toString()));
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

    private static com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility getGraphSONUtility(final Element element, final boolean edgeIn) {
        // this method and related caching can be replaced at blueprints 2.3.0 which has better key exclusion
        // support.  at that point the GraphSONUtility can be instantiated statically.  from an object creation
        // perspective this cached approach is almost as good when property keys are not highly disparate
        // across the graph.  even without caching the profiler showed that creation of GraphSONUtility is pretty
        // inexpensive anyway.
        final Set<String> elementPropertyKeys = new HashSet<String>(element.getPropertyKeys());
        elementPropertyKeys.add(GraphSONTokens._ID);
        if (element instanceof Edge) {
            if (edgeIn) {
                elementPropertyKeys.add(GraphSONTokens._IN_V);
            } else {
                elementPropertyKeys.add(GraphSONTokens._OUT_V);
            }

            if (!graphsonEdgeCache.containsKey(elementPropertyKeys)) {
                graphsonEdgeCache.put(elementPropertyKeys, new com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility(
                        GraphSONMode.COMPACT, elementFactory, ElementPropertyConfig.IncludeProperties(null, elementPropertyKeys)));
            }

            return graphsonEdgeCache.get(elementPropertyKeys);
        } else {
            if (!graphsonVertexCache.containsKey(elementPropertyKeys)) {
                graphsonVertexCache.put(elementPropertyKeys, new com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility(
                        GraphSONMode.COMPACT, elementFactory, ElementPropertyConfig.IncludeProperties(elementPropertyKeys, null)));
            }

            return graphsonVertexCache.get(elementPropertyKeys);
        }
    }

    private static class FaunusElementFactory implements ElementFactory<FaunusVertex, FaunusEdge> {
        @Override
        public FaunusEdge createEdge(final Object id, final FaunusVertex out, final FaunusVertex in, final String label) {
            return new FaunusEdge(convertIdentifier(id), out.getIdAsLong(), in.getIdAsLong(), label);
        }

        @Override
        public FaunusVertex createVertex(final Object id) {
            return new FaunusVertex(convertIdentifier(id));
        }

        private long convertIdentifier(final Object id) {
            if (id instanceof Long)
                return (Long) id;

            long identifier = -1l;
            if (id != null) {
                try {
                    identifier = Long.parseLong(id.toString());
                } catch (final NumberFormatException e) {
                    identifier = -1l;
                }
            }
            return identifier;
        }
    }
}