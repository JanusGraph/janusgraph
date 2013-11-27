package com.thinkaurelius.faunus;

import com.google.common.base.Preconditions;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ElementHelper;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusElement implements Vertex {

    public static final Direction[] PROPER_DIR = new Direction[]{IN,OUT};

    protected ListMultimap<FaunusType,FaunusEdge> outEdges = ArrayListMultimap.create();
    protected ListMultimap<FaunusType,FaunusEdge> inEdges = ArrayListMultimap.create();

    public FaunusVertex() {
        super(-1l);
    }

    public FaunusVertex(final boolean enablePaths) {
        super(-1l);
        this.enablePath(enablePaths);
    }

    public FaunusVertex(final long id) {
        super(id);
    }

    public FaunusVertex(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public FaunusVertex reuse(final long id) {
        super.reuse(id);
        this.outEdges.clear();
        this.inEdges.clear();
        return this;
    }

    public void addAll(final FaunusVertex vertex) {
        this.id = vertex.getIdAsLong();
        this.properties = vertex.properties;
        this.getPaths(vertex, false);
        this.addEdges(BOTH, vertex);
    }

    @Override
    void updateSchema(FaunusSerializer.Schema schema) {
        super.updateSchema(schema);
        for (Direction dir : PROPER_DIR) {
            for (FaunusEdge edge : getAdjacency(dir).values())
                edge.updateSchema(schema);
        }
    }

    //##################################
    // Property Handling
    //##################################


    public void addProperty(final String key, final Object value) {
        ElementHelper.validateProperty(this, key, value);
        FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        initializeProperties();
        this.properties.put(type, value);
    }

    public <T> Iterable<T> getProperties(final String key) {
        return (Iterable)properties.get(FaunusType.DEFAULT_MANAGER.get(key));
    }

    //##################################
    // Edge Handling
    //##################################

    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    private ListMultimap<FaunusType,FaunusEdge> getAdjacency(final Direction dir) {
        switch(dir) {
            case IN: return inEdges;
            case OUT: return outEdges;
            default: throw ExceptionFactory.bothIsNotSupported();
        }
    }

    public Set<FaunusType> getEdgeLabels(final Direction direction) {
        if (direction==BOTH) {
            return Sets.union(getEdgeLabels(IN),getEdgeLabels(OUT));
        } else {
            return getAdjacency(direction).keySet();
        }
    }

    public Iterable<Vertex> getVertices(final Direction direction, final String... labels) {
        return new Iterable<Vertex>() {
            public Iterator<Vertex> iterator() {
                return new Iterator<Vertex>() {
                    final Iterator<Edge> edges = getEdges(direction, labels).iterator();
                    final Direction opposite = direction.opposite();

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                    public boolean hasNext() {
                        return this.edges.hasNext();
                    }

                    public Vertex next() {
                        return this.edges.next().getVertex(this.opposite);
                    }
                };
            }
        };
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, String... labels) {
        if (labels==null) labels = new String[0];
        FaunusType[] types = new FaunusType[labels.length];
        for (int i=0;i<labels.length;i++) types[i]=FaunusType.DEFAULT_MANAGER.get(labels[i]);
        return getEdges(direction,types);
    }

    //Need to disambiguate the getEdges() methods when no label is specified.
    public Iterable<Edge> getEdges(final Direction direction) {
        return getEdges(direction,new FaunusType[0]);
    }

    public Iterable<Edge> getEdges(final Direction direction, final FaunusType... labels) {
        final List<List<Edge>> edgeLists = new ArrayList<List<Edge>>();

        for (Direction dir : PROPER_DIR) {
            if (direction!=BOTH && direction!=dir) continue;
            ListMultimap<FaunusType,FaunusEdge> adj = getAdjacency(dir);
            if (null == labels || labels.length == 0) {
                for (FaunusType type : adj.keySet()) edgeLists.add((List)adj.get(type));
            } else {
                for (final FaunusType label : labels) {
                    final List<Edge> temp = (List)adj.get(label);
                    edgeLists.add(temp);
                }
            }
        }
        return new EdgeList(edgeLists);
    }

    private void addEdges(final Direction direction, final FaunusType label, final List<FaunusEdge> edges) {
        getAdjacency(direction).putAll(label,edges);
    }

    public void addEdges(final Direction direction, final FaunusVertex vertex) {
        for (Direction dir : PROPER_DIR) {
            if (direction==dir || direction.equals(BOTH)) {
                for (final FaunusType label : vertex.getEdgeLabels(dir)) {
                    this.addEdges(dir, label, (List) vertex.getEdges(dir, label));
                }
            }
        }
    }

    public Edge addEdge(final String label, final Vertex inVertex) {
        return this.addEdge(Direction.OUT, new FaunusEdge(this.getIdAsLong(), ((FaunusVertex) inVertex).getIdAsLong(), label));
    }

    public Edge addEdge(final Direction direction, final String label, final long otherVertexId) {
        if (direction==OUT)
            return this.addEdge(OUT, new FaunusEdge(this.id, otherVertexId, label));
        else if (direction==Direction.IN)
            return this.addEdge(Direction.IN, new FaunusEdge(otherVertexId, this.id, label));
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    public FaunusEdge addEdge(final Direction direction, final FaunusEdge edge) {
        getAdjacency(direction).put(edge.getType(),edge);
        return edge;
    }

    public void removeEdgesToFrom(final Set<Long> ids) {
        for (Direction dir : PROPER_DIR) {
            Iterator<FaunusEdge> edges = getAdjacency(dir).values().iterator();
            while (edges.hasNext()) {
                if (ids.contains(edges.next().getVertexId(dir.opposite())))
                    edges.remove();
            }
        }
    }

    public void removeEdges(final Tokens.Action action, final Direction direction, final String... strlabels) {
        Set<FaunusType> labels = Sets.newHashSet();
        for (String label : strlabels) labels.add(FaunusType.DEFAULT_MANAGER.get(label));

        if (action.equals(Tokens.Action.KEEP)) {
            for (Direction dir : PROPER_DIR) {
                if (direction==BOTH || direction==dir) {
                    ListMultimap<FaunusType,FaunusEdge> adj = getAdjacency(dir);
                    if (labels.size() > 0) {
                        Set<FaunusType> removal = Sets.newHashSet(adj.keySet());
                        removal.removeAll(labels);
                        for (FaunusType type : removal) adj.removeAll(type);
                    } else if (direction==dir)
                        getAdjacency(dir.opposite()).clear();
                }
            }
        } else {
            assert action.equals(Tokens.Action.DROP);
            for (Direction dir : PROPER_DIR) {
                if (direction==BOTH || direction==dir) {
                    if (labels.isEmpty()) getAdjacency(dir).clear();
                    else {
                        for (FaunusType label: labels) getAdjacency(dir).removeAll(label);
                    }
                }
            }
        }
    }

    private class EdgeList extends AbstractList<Edge> {

        final List<List<Edge>> edges;

        int size;

        public EdgeList(final List<List<Edge>> edgeLists) {
            this.edges = edgeLists;
            int counter = 0;
            for (final List<Edge> temp : this.edges) {
                counter = counter + temp.size();
            }
            this.size = counter;
        }

        public int size() {
            return this.size;
        }

        public Edge get(final int index) {
            int lowIndex = 0;
            int highIndex = 0;
            for (final List<Edge> temp : this.edges) {
                highIndex = highIndex + temp.size();
                if (index < highIndex) {
                    return temp.get(index - lowIndex);
                }
                lowIndex = lowIndex + temp.size();
            }
            throw new ArrayIndexOutOfBoundsException(index);
        }

        private void removeList(final int index) {
            int lowIndex = 0;
            int highIndex = 0;
            for (final List<Edge> temp : this.edges) {
                highIndex = highIndex + temp.size();
                if (index < highIndex) {
                    temp.remove(index - lowIndex);
                    return;
                }
                lowIndex = lowIndex + temp.size();

            }
            throw new ArrayIndexOutOfBoundsException(index);
        }

        public Iterator<Edge> iterator() {
            return new Iterator<Edge>() {
                private int index = 0;

                @Override
                public boolean hasNext() {
                    return this.index < size;
                }

                @Override
                public Edge next() {
                    return get(this.index++);
                }

                @Override
                public void remove() {
                    removeList(--this.index);
                    size--;
                }
            };
        }

    }

    //##################################
    // Serialization Proxy
    //##################################

    public void write(final DataOutput out) throws IOException {
        FaunusSerializer.DEFAULT_SERIALIZER.writeVertex(this, out);
    }

    public void readFields(final DataInput in) throws IOException {
        FaunusSerializer.DEFAULT_SERIALIZER.readVertex(this, in);
    }

    //##################################
    // General Utility
    //##################################

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }


    //##################################
    // Path Handling
    //##################################


    @Override
    public void enablePath(final boolean enablePath) {
        super.enablePath(enablePath);
        if (this.pathEnabled) {
            for (final Edge edge : this.getEdges(BOTH)) {
                ((FaunusEdge) edge).enablePath(true);
            }
        }
    }


    public static class MicroVertex extends MicroElement {

        private static final String V1 = "v[";
        private static final String V2 = "]";

        public MicroVertex(final long id) {
            super(id);
        }

        public String toString() {
            return V1 + this.id + V2;
        }
    }
}
