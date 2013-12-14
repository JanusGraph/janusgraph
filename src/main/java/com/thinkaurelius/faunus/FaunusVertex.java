package com.thinkaurelius.faunus;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.VertexQuery;
import com.tinkerpop.blueprints.util.DefaultVertexQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nullable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusPathElement implements Vertex {

    protected ListMultimap<FaunusType, FaunusEdge> outEdges = ArrayListMultimap.create();
    protected ListMultimap<FaunusType, FaunusEdge> inEdges = ArrayListMultimap.create();

    public FaunusVertex() {
        super(-1);
    }

    public FaunusVertex(final Configuration configuration) {
        super(-1l);
        this.setConf(configuration);
    }

    public FaunusVertex(final Configuration configuration, final long id) {
        super(id);
        this.setConf(configuration);
    }

    public FaunusVertex(final Configuration configuration, final DataInput in) throws IOException {
        super(-1l);
        this.setConf(configuration);
        this.readFields(in);
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
        for (Direction dir : Direction.proper) {
            for (FaunusEdge edge : getAdjacency(dir).values())
                edge.updateSchema(schema);
        }
    }

    //##################################
    // Property Handling
    //##################################

    @Override
    public FaunusProperty addProperty(FaunusProperty property) {
        return super.addProperty(property);
    }

    public FaunusProperty addProperty(final String key, final Object value) {
        FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        return addProperty(new FaunusProperty(type, value));
    }

    public <T> Iterable<T> getProperties(final String key) {
        FaunusType type = FaunusType.DEFAULT_MANAGER.get(key);
        if (type.isImplicit()) return getImplicitProperty(type);
        return Iterables.transform(Iterables.filter(properties.get(type), FILTER_DELETED_PROPERTIES), new Function<FaunusProperty, T>() {
            @Nullable
            @Override
            public T apply(@Nullable FaunusProperty faunusProperty) {
                return (T) faunusProperty.getValue();
            }
        });
    }

    public Iterable<FaunusProperty> getProperties(final FaunusType type) {
        Preconditions.checkArgument(!type.isImplicit());
        return Iterables.filter(properties.get(type), FILTER_DELETED_PROPERTIES);
    }

    //##################################
    // Edge Handling
    //##################################

    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    private ListMultimap<FaunusType, FaunusEdge> getAdjacency(final Direction dir) {
        switch (dir) {
            case IN:
                return inEdges;
            case OUT:
                return outEdges;
            default:
                throw ExceptionFactory.bothIsNotSupported();
        }
    }

    static boolean containsUndeletedEdge(ListMultimap<FaunusType, FaunusEdge> adj, FaunusType type) {
        return !Iterables.isEmpty(Iterables.filter(adj.get(type), FILTER_DELETED_EDGES));
    }

    public Set<FaunusType> getEdgeLabels(final Direction direction) {
        if (direction == BOTH) {
            return Sets.union(getEdgeLabels(IN), getEdgeLabels(OUT));
        } else {
            return Sets.filter(getAdjacency(direction).keySet(), new Predicate<FaunusType>() {
                @Override
                public boolean apply(@Nullable FaunusType faunusType) {
                    return !faunusType.isHidden() && containsUndeletedEdge(getAdjacency(direction), faunusType);
                }
            });
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

    public Iterable<FaunusEdge> getEdgesWithState(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return this.outEdges.values();
        else
            return this.inEdges.values();
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, String... labels) {
        if (labels == null) labels = new String[0];
        FaunusType[] types = new FaunusType[labels.length];
        for (int i = 0; i < labels.length; i++) types[i] = FaunusType.DEFAULT_MANAGER.get(labels[i]);
        return getEdges(direction, types);
    }

    //Need to disambiguate the getEdges() methods when no label is specified.
    public Iterable<Edge> getEdges(final Direction direction) {
        return getEdges(direction, new FaunusType[0]);
    }

    public Iterable<Edge> getEdges(final Direction direction, final FaunusType... labels) {
        final List<List<FaunusEdge>> edgeLists = new ArrayList<List<FaunusEdge>>();

        for (final Direction dir : Direction.proper) {
            if (direction != BOTH && direction != dir) continue;
            ListMultimap<FaunusType, FaunusEdge> adj = getAdjacency(dir);
            if (null == labels || labels.length == 0) {
                for (FaunusType type : adj.keySet()) {
                    if (!type.isHidden()) edgeLists.add((List) adj.get(type));
                }
            } else {
                for (final FaunusType label : labels) {
                    final List<FaunusEdge> temp = adj.get(label);
                    edgeLists.add(temp);
                }
            }
        }
        return (Iterable) new EdgeList(edgeLists);
    }


    private void addEdges(final Direction direction, final FaunusType label, final List<FaunusEdge> edges) {
        getAdjacency(direction).putAll(label, edges);
    }

    public void addEdges(final Direction direction, final FaunusVertex vertex) {
        for (final Direction dir : Direction.proper) {
            if (direction == dir || direction.equals(BOTH)) {
                for (final FaunusType label : vertex.getEdgeLabels(dir)) {
                    this.addEdges(dir, label, (List) vertex.getEdges(dir, label));
                }
            }
        }
    }

    public Edge addEdge(final String label, final Vertex inVertex) {
        return this.addEdge(Direction.OUT, new FaunusEdge(this.configuration, this.getIdAsLong(), ((FaunusVertex) inVertex).getIdAsLong(), label));
    }

    public Edge addEdge(final Direction direction, final String label, final long otherVertexId) {
        if (direction == OUT)
            return this.addEdge(OUT, new FaunusEdge(this.configuration, this.id, otherVertexId, label));
        else if (direction == Direction.IN)
            return this.addEdge(Direction.IN, new FaunusEdge(this.configuration, otherVertexId, this.id, label));
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    public FaunusEdge addEdge(final Direction direction, final FaunusEdge edge) {
        edge.setConf(this.getConf());
        getAdjacency(direction).put(edge.getType(), edge);
        return edge;
    }

    public void removeEdgesToFrom(final Set<Long> ids) {
        for (final Direction dir : Direction.proper) {
            Iterator<FaunusEdge> edges = getAdjacency(dir).values().iterator();
            while (edges.hasNext()) {
                FaunusEdge edge = edges.next();
                if (ids.contains(edge.getVertexId(dir.opposite()))) {
                    if (edge.isNew()) edges.remove();
                    edge.setState(ElementState.DELETED);
                }
            }
        }
    }

    private void removeAllEdges(final Direction dir, Iterable<FaunusType> types) {
        types = Lists.newArrayList(types);
        ListMultimap<FaunusType, FaunusEdge> adj = getAdjacency(dir);
        for (FaunusType type : types) {
            Iterator<FaunusEdge> iter = adj.get(type).iterator();
            while (iter.hasNext()) {
                FaunusEdge edge = iter.next();
                if (edge.isNew()) iter.remove();
                edge.setState(ElementState.DELETED);
            }
        }
    }

    public void removeEdges(final Tokens.Action action, final Direction direction, final String... stringLabels) {
        Set<FaunusType> labels = Sets.newHashSet();
        for (String label : stringLabels) labels.add(FaunusType.DEFAULT_MANAGER.get(label));

        if (action.equals(Tokens.Action.KEEP)) {
            for (Direction dir : Direction.proper) {
                if (direction == BOTH || direction == dir) {
                    ListMultimap<FaunusType, FaunusEdge> adj = getAdjacency(dir);
                    if (labels.size() > 0) {
                        Set<FaunusType> removal = Sets.newHashSet(adj.keySet());
                        removal.removeAll(labels);
                        removeAllEdges(dir, removal);
                    } else if (direction == dir)
                        removeAllEdges(dir.opposite(), getAdjacency(dir.opposite()).keySet());
                }
            }
        } else {
            assert action.equals(Tokens.Action.DROP);
            for (Direction dir : Direction.proper) {
                if (direction == BOTH || direction == dir) {
                    if (labels.isEmpty()) removeAllEdges(dir, getAdjacency(dir).keySet());
                    else {
                        removeAllEdges(dir, labels);
                    }
                }
            }
        }
    }

    private class EdgeList extends AbstractList<FaunusEdge> {

        final List<List<FaunusEdge>> edges;

        int fullsize;
        int size;

        public EdgeList(final List<List<FaunusEdge>> edgeLists) {
            this.edges = edgeLists;
            fullsize = 0;
            size = 0;
            for (final List<FaunusEdge> temp : this.edges) {
                fullsize += temp.size();
                for (FaunusEdge e : temp) if (!e.isDeleted()) size++;
            }
        }

        public int size() {
            return this.size;
        }

        public FaunusEdge get(final int index) {
            throw new UnsupportedOperationException();
        }

        public FaunusEdge getDirect(final int index) {
            int lowIndex = 0;
            int highIndex = 0;
            for (final List<FaunusEdge> temp : this.edges) {
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
            for (final List<FaunusEdge> temp : this.edges) {
                highIndex = highIndex + temp.size();
                if (index < highIndex) {
                    temp.remove(index - lowIndex);
                    return;
                }
                lowIndex = lowIndex + temp.size();

            }
            throw new ArrayIndexOutOfBoundsException(index);
        }

        public Iterator<FaunusEdge> iterator() {
            return new Iterator<FaunusEdge>() {
                private int current = -1;
                private int next = findNext(current);

                private int findNext(int current) {
                    int next = current + 1;
                    while (next < fullsize && getDirect(next).isDeleted()) next++;
                    return next;
                }

                @Override
                public boolean hasNext() {
                    return this.next < fullsize;
                }

                @Override
                public FaunusEdge next() {
                    current = next;
                    next = findNext(current);
                    return getDirect(current);
                }

                @Override
                public void remove() {
                    FaunusEdge toDelete = getDirect(current);
                    if (toDelete.isNew()) {
                        removeList(current);
                        next--;
                        fullsize--;
                    }
                    toDelete.setState(ElementState.DELETED);
                    current = -1;
                    size--;
                }
            };
        }

    }

    //##################################
    // Serialization Proxy
    //##################################

    public void write(final DataOutput out) throws IOException {
        new FaunusSerializer(this.getConf()).writeVertex(this, out);
    }

    public void readFields(final DataInput in) throws IOException {
        new FaunusSerializer(this.getConf()).readVertex(this, in);
    }

    //##################################
    // General Utility
    //##################################

    @Override
    public String toString() {
        return StringFactory.vertexString(this);
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
