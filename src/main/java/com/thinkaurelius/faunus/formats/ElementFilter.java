package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.Tokens;
import org.apache.hadoop.conf.Configuration;

import java.io.BufferedOutputStream;
import java.io.BufferedWriter;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class ElementFilter {

    public Set<String> edgeLabels = null;
    public Tokens.Action edgeLabelsAction = null;
    public Set<String> edgeKeys = null;
    public Tokens.Action edgeKeysAction = null;
    public Set<String> vertexKeys = null;
    public Tokens.Action vertexKeysAction = null;

    public ElementFilter(final Configuration configuration) {
        String[] strings = configuration.getStrings(Tokens.GRAPH_INPUT_EDGE_LABEL_FILTER_LABELS);
        if (null != strings && strings.length > 0)
            this.edgeLabels = new HashSet<String>(Arrays.asList(strings));

        strings = configuration.getStrings(Tokens.GRAPH_INPUT_EDGE_PROPERTY_FILTER_KEYS);
        if (null != strings && strings.length > 0)
            this.edgeKeys = new HashSet<String>(Arrays.asList(strings));

        strings = configuration.getStrings(Tokens.GRAPH_INPUT_VERTEX_PROPERTY_FILTER_KEYS);
        if (null != strings && strings.length > 0)
            this.vertexKeys = new HashSet<String>(Arrays.asList(strings));

        String string = configuration.get(Tokens.GRAPH_INPUT_EDGE_LABEL_FILTER_ACTION);
        if (null != string)
            this.edgeLabelsAction = Tokens.Action.valueOf(string);

        string = configuration.get(Tokens.GRAPH_INPUT_EDGE_PROPERTY_FILTER_ACTION);
        if (null != string)
            this.edgeKeysAction = Tokens.Action.valueOf(string);

        string = configuration.get(Tokens.GRAPH_INPUT_VERTEX_PROPERTY_FILTER_ACTION);
        if (null != string)
            this.vertexKeysAction = Tokens.Action.valueOf(string);
    }

    public boolean filterEdgeLabels() {
        return null != this.edgeLabels && null != this.edgeLabelsAction;
    }

    public boolean filterEdgeProperties() {
        return null != this.edgeKeys && null != this.edgeKeysAction;
    }

    public boolean filterVertexProperties() {
        return null != this.vertexKeys && null != this.vertexKeysAction;
    }

    public Set<String> getEdgeLabels() {
        return this.edgeLabels;
    }

    public Tokens.Action getEdgeLabelsAction() {
        return this.edgeLabelsAction;
    }

    public Set<String> getEdgeKeys() {
        return this.edgeKeys;
    }

    public Tokens.Action getEdgeKeysAction() {
        return this.edgeKeysAction;
    }

    public Set<String> getVertexKeys() {
        return this.vertexKeys;
    }

    public Tokens.Action getVertexKeysAction() {
        return this.vertexKeysAction;
    }
}
