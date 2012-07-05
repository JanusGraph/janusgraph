package com.thinkaurelius.faunus.io.formats.json;

import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusJSONUtility {

    private static final JSONParser parser = new JSONParser();

    public static List<FaunusVertex> fromJSON(final InputStream in) throws IOException {
        final List<FaunusVertex> vertices = new LinkedList<FaunusVertex>();
        final BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = bfs.readLine()) != null) {
            vertices.add(FaunusJSONUtility.fromJSON(line));
        }
        bfs.close();
        return vertices;

    }

    public static FaunusVertex fromJSON(final String line) throws IOException {
        try {
            final JSONObject json = (JSONObject) FaunusJSONUtility.parser.parse(line);
            final FaunusVertex vertex = new FaunusVertex(Long.valueOf(json.get(JSONTokens.ID).toString()));
            final JSONObject properties = (JSONObject) json.get(JSONTokens.PROPERTIES);
            if (null != properties) {
                for (final Object key : properties.keySet()) {
                    vertex.setProperty((String) key, properties.get(key));
                }
            }

            final JSONArray outEdges = (JSONArray) json.get(JSONTokens.OUT_E);
            if (null != outEdges) {
                final Iterator itty = outEdges.iterator();
                while (itty.hasNext()) {
                    final JSONObject outEdge = (JSONObject) itty.next();
                    final long id;
                    final Object temp = outEdge.get(JSONTokens.ID);
                    if (null != temp)
                        id = Long.valueOf(temp.toString());
                    else
                        id = -1l;
                    final long inVertexId = Long.valueOf(outEdge.get(JSONTokens.IN_ID).toString());
                    final String label = (String) outEdge.get(JSONTokens.LABEL);
                    final FaunusEdge edge = new FaunusEdge(id, vertex, new FaunusVertex(inVertexId), label);
                    final JSONObject edgeProperties = (JSONObject) outEdge.get(JSONTokens.PROPERTIES);
                    if (null != edgeProperties) {
                        for (final Object key : edgeProperties.keySet()) {
                            edge.setProperty((String) key, edgeProperties.get(key));
                        }
                    }
                    vertex.addEdge(OUT, edge);
                }
            }

            final JSONArray inEdges = (JSONArray) json.get(JSONTokens.IN_E);
            if (null != inEdges) {
                final Iterator itty = inEdges.iterator();
                while (itty.hasNext()) {
                    final JSONObject inEdge = (JSONObject) itty.next();

                    final long id;
                    final Object temp = inEdge.get(JSONTokens.ID);
                    if (null != temp)
                        id = Long.valueOf(temp.toString());
                    else
                        id = -1l;
                    final long outVertexId = Long.valueOf(inEdge.get(JSONTokens.OUT_ID).toString());
                    final String label = (String) inEdge.get(JSONTokens.LABEL);
                    final FaunusEdge edge = new FaunusEdge(id, new FaunusVertex(outVertexId), vertex, label);
                    final JSONObject edgeProperties = (JSONObject) inEdge.get(JSONTokens.PROPERTIES);
                    if (null != edgeProperties) {
                        for (final Object key : edgeProperties.keySet()) {
                            edge.setProperty((String) key, edgeProperties.get(key));
                        }
                    }
                    vertex.addEdge(IN, edge);
                }
            }

            return vertex;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }

    public static JSONObject toJSON(final Vertex vertex) {
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

        List<Edge> edges = (List<Edge>) vertex.getEdges(OUT);
        if (!edges.isEmpty()) {
            final JSONArray outEdgesArray = new JSONArray();
            for (final Edge outEdge : edges) {
                final JSONObject edge = new JSONObject();
                edge.put(JSONTokens.ID, outEdge.getId());
                edge.put(JSONTokens.IN_ID, outEdge.getVertex(IN).getId());
                edge.put(JSONTokens.OUT_ID, outEdge.getVertex(OUT).getId());
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

        edges = (List<Edge>) vertex.getEdges(IN);
        if (!edges.isEmpty()) {
            final JSONArray inEdgesArray = new JSONArray();
            for (final Edge inEdge : edges) {
                final JSONObject edge = new JSONObject();
                edge.put(JSONTokens.ID, inEdge.getId());
                edge.put(JSONTokens.IN_ID, inEdge.getVertex(IN).getId());
                edge.put(JSONTokens.OUT_ID, inEdge.getVertex(OUT).getId());
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
        return object;
    }
}
