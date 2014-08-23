package com.thinkaurelius.titan.hadoop.formats.graphson;

import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.hadoop.FaunusVertex;
import com.thinkaurelius.titan.hadoop.StandardFaunusEdge;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.io.graphson.ElementFactory;
import com.tinkerpop.blueprints.util.io.graphson.ElementPropertyConfig;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONMode;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONTokens;
import com.tinkerpop.blueprints.util.io.graphson.GraphSONUtility;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.codehaus.jettison.json.JSONTokener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class HadoopGraphSONUtility {

    private static final Logger log =
            LoggerFactory.getLogger(HadoopGraphSONUtility.class);

    private static final String _OUT_E = "_outE";
    private static final String _IN_E = "_inE";
    private static final ImmutableSet<String> VERTEX_IGNORE = ImmutableSet.of(GraphSONTokens._TYPE, _OUT_E, _IN_E);
    private static final ImmutableSet<String> EDGE_IGNORE = ImmutableSet.of(GraphSONTokens._TYPE, GraphSONTokens._OUT_V, GraphSONTokens._IN_V);

    private final HadoopElementFactory elementFactory;
    private final GraphSONUtility graphson;

    public HadoopGraphSONUtility(Configuration configuration) {
        elementFactory = new HadoopElementFactory(configuration);
        graphson = new GraphSONUtility(GraphSONMode.COMPACT, elementFactory,
                ElementPropertyConfig.excludeProperties(VERTEX_IGNORE, EDGE_IGNORE));
    }

    public List<FaunusVertex> fromJSON(final InputStream in) throws IOException {
        final List<FaunusVertex> vertices = new LinkedList<FaunusVertex>();
        final BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = bfs.readLine()) != null) {
            vertices.add(fromJSON( line));
        }
        bfs.close();
        return vertices;

    }

    public FaunusVertex fromJSON(String line) throws IOException {
        try {
            final JSONObject json = new JSONObject(new JSONTokener(line));

            final FaunusVertex vertex = (FaunusVertex) graphson.vertexFromJson(json);

            fromJSONEdges(vertex, json.optJSONArray(_OUT_E), OUT);
            json.remove(_OUT_E); // clear up some memory
            fromJSONEdges(vertex, json.optJSONArray(_IN_E), IN);
            json.remove(_IN_E); // clear up some memory

            return vertex;
        } catch (Exception e) {
            log.error("JSON parse exception", e);
            throw new IOException(e.getMessage(), e);
        }
    }

    private void fromJSONEdges(final FaunusVertex vertex, final JSONArray edges, final Direction direction) throws JSONException, IOException {
        if (null != edges) {
            for (int i = 0; i < edges.length(); i++) {
                final JSONObject edge = edges.optJSONObject(i);
                StandardFaunusEdge faunusEdge = null;
                if (direction.equals(Direction.IN)) {
                    faunusEdge = (StandardFaunusEdge) graphson.edgeFromJson(edge, new FaunusVertex(vertex.getFaunusConf(), edge.optLong(GraphSONTokens._OUT_V)), vertex);
                } else if (direction.equals(Direction.OUT)) {
                    faunusEdge = (StandardFaunusEdge) graphson.edgeFromJson(edge, vertex, new FaunusVertex(vertex.getFaunusConf(), edge.optLong(GraphSONTokens._IN_V)));
                }

                if (faunusEdge != null) {
                    vertex.addEdge(direction, faunusEdge);
                }
            }
        }
    }

    public JSONObject toJSON(final Vertex vertex) throws IOException {
        try {
            final JSONObject object = GraphSONUtility.jsonFromElement(vertex, getElementPropertyKeys(vertex, false), GraphSONMode.COMPACT);

            // force the ID to long.  with blueprints, most implementations will send back a long, but
            // some like TinkerGraph will return a string.  the same is done for edges below
            object.put(GraphSONTokens._ID, Long.valueOf(object.remove(GraphSONTokens._ID).toString()));

            Iterator<Edge> edges = vertex.getEdges(OUT).iterator();
            if (edges.hasNext()) {
                final JSONArray outEdgesArray = new JSONArray();
                while (edges.hasNext()) {
                    Edge outEdge = edges.next();
                    final JSONObject edgeObject = GraphSONUtility.jsonFromElement(outEdge, getElementPropertyKeys(outEdge, true), GraphSONMode.COMPACT);
                    outEdgesArray.put(edgeObject);
                }
                object.put(_OUT_E, outEdgesArray);
            }

            edges = vertex.getEdges(IN).iterator();
            if (edges.hasNext()) {
                final JSONArray inEdgesArray = new JSONArray();
                while (edges.hasNext()) {
                    Edge inEdge = edges.next();
                    final JSONObject edgeObject = GraphSONUtility.jsonFromElement(inEdge, getElementPropertyKeys(inEdge, false), GraphSONMode.COMPACT);
                    inEdgesArray.put(edgeObject);
                }
                object.put(_IN_E, inEdgesArray);
            }

            return object;
        } catch (JSONException e) {
            throw new IOException(e);
        }
    }

    private static Set<String> getElementPropertyKeys(final Element element, final boolean edgeIn) {
        final Set<String> elementPropertyKeys = new HashSet<String>(element.getPropertyKeys());
        elementPropertyKeys.add(GraphSONTokens._ID);
        if (element instanceof Edge) {
            if (edgeIn) {
                elementPropertyKeys.add(GraphSONTokens._IN_V);
            } else {
                elementPropertyKeys.add(GraphSONTokens._OUT_V);
            }

            elementPropertyKeys.add(GraphSONTokens._LABEL);
        }

        return elementPropertyKeys;
    }

    private static class HadoopElementFactory implements ElementFactory<FaunusVertex, StandardFaunusEdge> {

        private Configuration configuration;

        private HadoopElementFactory(Configuration configuration) {
            this.configuration = configuration;
        }

        @Override
        public StandardFaunusEdge createEdge(final Object id, final FaunusVertex out, final FaunusVertex in, final String label) {
            return new StandardFaunusEdge(configuration, convertIdentifier(id), out.getLongId(), in.getLongId(), label);
        }

        @Override
        public FaunusVertex createVertex(final Object id) {
            return new FaunusVertex(configuration, convertIdentifier(id));
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
