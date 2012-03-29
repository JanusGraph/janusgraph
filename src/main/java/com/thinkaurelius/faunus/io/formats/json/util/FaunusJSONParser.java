package com.thinkaurelius.faunus.io.formats.json.util;

import com.thinkaurelius.faunus.io.formats.json.JSONTokens;
import com.thinkaurelius.faunus.io.graph.FaunusEdge;
import com.thinkaurelius.faunus.io.graph.FaunusVertex;
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

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusJSONParser {

    private final JSONParser parser = new JSONParser();

    public List<FaunusVertex> parse(final InputStream in) throws IOException {
        List<FaunusVertex> vertices = new LinkedList<FaunusVertex>();
        BufferedReader bfs = new BufferedReader(new InputStreamReader(in));
        String line = "";
        while ((line = bfs.readLine()) != null) {
            vertices.add(this.parse(line));
        }
        return vertices;

    }

    public FaunusVertex parse(final String line) throws IOException {
        try {
            final JSONObject json = (JSONObject) this.parser.parse(line);
            final FaunusVertex vertex = new FaunusVertex((Long) json.get(JSONTokens.ID));
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
                    final long inVertexId = (Long) outEdge.get(JSONTokens.IN_ID);
                    final String label = (String) outEdge.get(JSONTokens.LABEL);
                    final FaunusEdge edge = new FaunusEdge(vertex, new FaunusVertex(inVertexId), label);
                    final JSONObject edgeProperties = (JSONObject) outEdge.get(JSONTokens.PROPERTIES);
                    if (null != edgeProperties) {
                        for (final Object key : edgeProperties.keySet()) {
                            edge.setProperty((String) key, edgeProperties.get(key));
                        }
                    }
                    vertex.addOutEdge(edge);
                }
            }
            return vertex;
        } catch (Exception e) {
            throw new IOException(e.getMessage(), e);
        }
    }
}
