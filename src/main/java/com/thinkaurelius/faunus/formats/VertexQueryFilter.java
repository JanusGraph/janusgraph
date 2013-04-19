package com.thinkaurelius.faunus.formats;

import com.thinkaurelius.faunus.FaunusVertex;
import com.thinkaurelius.faunus.Tokens;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.gremlin.groovy.jsr223.GremlinGroovyScriptEngine;
import org.apache.hadoop.conf.Configuration;

import java.util.Iterator;
import java.util.Set;

/**
 * A filtering mechanism for, if possible, only getting particular aspects of a vertex from the graph.
 * A minimum, the filtering is done on the Hadoop side of the computation.
 * Makes use of VertexQuery from Blueprints to express such constraints.
 *
 * @author Marko A. Rodriguez (marko@thinkaurelius.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class VertexQueryFilter extends DefaultVertexQuery {

    public static final String FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER = "faunus.graph.input.vertex-query-filter";

    private static final GremlinGroovyScriptEngine engine = new GremlinGroovyScriptEngine();
    private static final String V = "v";
    private static final DummyVertex DUMMY_VERTEX = new DummyVertex();

    private boolean doesFilter = false;

    protected VertexQueryFilter() {
        super(DUMMY_VERTEX);
    }

    public static VertexQueryFilter create(final Configuration configuration) {
        engine.put(V, DUMMY_VERTEX);
        try {
            VertexQueryFilter query = (VertexQueryFilter) engine.eval(configuration.get(FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER, "v.query()"));
            if (null != configuration.get(FAUNUS_GRAPH_INPUT_VERTEX_QUERY_FILTER))
                query.setDoesFilter(true);
            return query;
        } catch (final Exception e) {
            throw new RuntimeException("VertexQueryFilter compilation error: " + e.getMessage(), e);
        }
    }

    protected void setDoesFilter(final boolean doesFilter) {
        this.doesFilter = doesFilter;
    }

    public boolean doesFilter() {
        return this.doesFilter;
    }

    public Iterable<Edge> edges() {
        throw new UnsupportedOperationException("This VertexQuery is used for graph filtering, not edge iteration");
    }

    public Iterable<Vertex> vertices() {
        throw new UnsupportedOperationException("This VertexQuery is used for graph filtering, not vertex iteration");
    }

    public Object vertexIds() {
        throw new UnsupportedOperationException();
    }

    public long count() {
        throw new UnsupportedOperationException();
    }

    public void defaultFilter(final FaunusVertex vertex) {
        if (!this.doesFilter) return;
        vertex.removeEdges(Tokens.Action.KEEP, this.direction, this.labels);
        Iterator<Edge> itty = vertex.getEdges(this.direction).iterator();
        Edge edge;
        while (itty.hasNext()) {
            edge = itty.next();
            for (final HasContainer hasContainer : this.hasContainers) {
                if (!hasContainer.isLegal(edge))
                    itty.remove();
            }
        }
        itty = vertex.getEdges(this.direction).iterator();
        long counter = 0;
        while (itty.hasNext()) {
            itty.next();
            if (++counter > this.limit)
                itty.remove();
        }
    }

    protected static class DummyVertex implements Vertex {

        public VertexQuery query() {
            return new VertexQueryFilter();
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        public Edge addEdge(final String label, Vertex inVertex) {
            throw new UnsupportedOperationException();
        }

        public Iterable<Edge> getEdges(Direction direction, String... labels) {
            throw new UnsupportedOperationException();
        }

        public Iterable<Vertex> getVertices(Direction direction, String... labels) {
            throw new UnsupportedOperationException();
        }

        public Set<String> getPropertyKeys() {
            throw new UnsupportedOperationException();
        }

        public <T> T getProperty(final String key) {
            throw new UnsupportedOperationException();
        }

        public void setProperty(final String key, final Object value) {
            throw new UnsupportedOperationException();
        }

        public <T> T removeProperty(final String key) {
            throw new UnsupportedOperationException();
        }

        public Object getId() {
            throw new UnsupportedOperationException();
        }
    }


}
