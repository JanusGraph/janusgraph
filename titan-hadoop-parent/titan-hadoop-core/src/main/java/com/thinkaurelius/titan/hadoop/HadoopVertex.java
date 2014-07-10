package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.hadoop.mapreduce.util.EmptyConfiguration;
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
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class HadoopVertex extends FaunusPathElement implements Vertex {

    protected ListMultimap<HadoopType, StandardFaunusEdge> outEdges = ArrayListMultimap.create();
    protected ListMultimap<HadoopType, StandardFaunusEdge> inEdges = ArrayListMultimap.create();

    public HadoopVertex() {
        super(EmptyConfiguration.immutable(), -1l);
    }

    public HadoopVertex(final Configuration configuration) {
        super(configuration, -1l);
    }

    public HadoopVertex(final Configuration configuration, final long id) {
        super(configuration, id);
    }

    public HadoopVertex(final Configuration configuration, final DataInput in) throws IOException {
        super(configuration, -1l);
        this.readFields(in);
    }

    public void addAll(final HadoopVertex vertex) {
        this.id = vertex.getLongId();
        this.adjacency = vertex.adjacency;
        this.getPaths(vertex, false);
        this.state = vertex.getLifeCycle();
        this.addEdges(BOTH, vertex);
    }

    @Override
    void updateSchema(final HadoopSerializer.Schema schema) {
        super.updateSchema(schema);
        for (Direction dir : Direction.proper) {
            for (StandardFaunusEdge edge : getAdjacency(dir).values())
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
        HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        return addProperty(new FaunusProperty(type, value));
    }

    public <T> Iterable<T> getProperties(final String key) {
        HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        if (type.isImplicit()) return Arrays.<T>asList((T)this.getImplicitProperty(type)); // TODO: is this okay?
        return Iterables.transform(Iterables.filter(adjacency.get(type), FILTER_DELETED_PROPERTIES), new Function<FaunusProperty, T>() {
            @Nullable
            @Override
            public T apply(@Nullable FaunusProperty faunusProperty) {
                return (T) faunusProperty.getValue();
            }
        });
    }

    public Iterable<FaunusProperty> getProperties(final HadoopType type) {
        Preconditions.checkArgument(!type.isImplicit());
        return Iterables.filter(adjacency.get(type), FILTER_DELETED_PROPERTIES);
    }

    //##################################
    // Edge Handling
    //##################################

    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    private ListMultimap<HadoopType, StandardFaunusEdge> getAdjacency(final Direction direction) {
        switch (direction) {
            case IN:
                return inEdges;
            case OUT:
                return outEdges;
            default:
                throw ExceptionFactory.bothIsNotSupported();
        }
    }

    static boolean containsUndeletedEdge(final ListMultimap<HadoopType, StandardFaunusEdge> edgeList, final HadoopType type) {
        return !Iterables.isEmpty(Iterables.filter(edgeList.get(type), FILTER_DELETED_EDGES));
    }

    public Set<HadoopType> getEdgeLabels(final Direction direction) {
        if (direction == BOTH) {
            return Sets.union(getEdgeLabels(IN), getEdgeLabels(OUT));
        } else {
            return Sets.filter(getAdjacency(direction).keySet(), new Predicate<HadoopType>() {
                @Override
                public boolean apply(final HadoopType hadoopType) {
                    return !hadoopType.isHidden() && containsUndeletedEdge(getAdjacency(direction), hadoopType);
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

    public Iterable<StandardFaunusEdge> getEdgesWithState(final Direction direction) {
        return getAdjacency(direction).values();
    }

    @Override
    public Iterable<Edge> getEdges(final Direction direction, String... labels) {
        if (labels == null) labels = new String[0];
        HadoopType[] types = new HadoopType[labels.length];
        for (int i = 0; i < labels.length; i++) types[i] = HadoopType.DEFAULT_MANAGER.get(labels[i]);
        return getEdges(direction, types);
    }

    //Need to disambiguate the getEdges() methods when no label is specified.
    public Iterable<Edge> getEdges(final Direction direction) {
        return getEdges(direction, new HadoopType[0]);
    }

    public Iterable<Edge> getEdges(final Direction direction, final HadoopType... labels) {
        final List<List<StandardFaunusEdge>> edgeLists = new ArrayList<List<StandardFaunusEdge>>();

        for (final Direction dir : Direction.proper) {
            if (direction != BOTH && direction != dir) continue;
            ListMultimap<HadoopType, StandardFaunusEdge> adj = getAdjacency(dir);
            if (null == labels || labels.length == 0) {
                for (HadoopType type : adj.keySet()) {
                    if (!type.isHidden()) edgeLists.add((List) adj.get(type));
                }
            } else {
                for (final HadoopType label : labels) {
                    final List<StandardFaunusEdge> temp = adj.get(label);
                    edgeLists.add(temp);
                }
            }
        }
        return (Iterable) new EdgeList(edgeLists);
    }


    private void addEdges(final Direction direction, final HadoopType label, final List<StandardFaunusEdge> edges) {
        getAdjacency(direction).putAll(label, edges);
    }

    public void addEdges(final Direction direction, final HadoopVertex vertex) {
        for (final Direction dir : Direction.proper) {
            if (direction == dir || direction.equals(BOTH)) {
                for (final HadoopType label : vertex.getEdgeLabels(dir)) {
                    this.addEdges(dir, label, (List) vertex.getEdges(dir, label));
                }
            }
        }
    }

    public Edge addEdge(final String label, final Vertex inVertex) {
        return this.addEdge(Direction.OUT, new StandardFaunusEdge(this.configuration, this.getLongId(), ((HadoopVertex) inVertex).getLongId(), label));
    }

    public Edge addEdge(final Direction direction, final String label, final long otherVertexId) {
        if (direction == OUT)
            return this.addEdge(OUT, new StandardFaunusEdge(this.configuration, this.id, otherVertexId, label));
        else if (direction == Direction.IN)
            return this.addEdge(Direction.IN, new StandardFaunusEdge(this.configuration, otherVertexId, this.id, label));
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    public StandardFaunusEdge addEdge(final Direction direction, final StandardFaunusEdge edge) {
        edge.setConf(this.getConf());
        getAdjacency(direction).put(edge.getType(), edge);
        return edge;
    }

    public void removeEdgesToFrom(final Set<Long> ids) {
        for (final Direction dir : Direction.proper) {
            Iterator<StandardFaunusEdge> edges = getAdjacency(dir).values().iterator();
            while (edges.hasNext()) {
                StandardFaunusEdge edge = edges.next();
                if (ids.contains(edge.getVertexId(dir.opposite()))) {
                    if (edge.isNew()) edges.remove();
                    edge.setLifeCycle(ElementState.DELETED);
                }
            }
        }
    }

    private void removeAllEdges(final Direction dir, Iterable<HadoopType> types) {
        types = Lists.newArrayList(types);
        ListMultimap<HadoopType, StandardFaunusEdge> adj = getAdjacency(dir);
        for (HadoopType type : types) {
            Iterator<StandardFaunusEdge> iter = adj.get(type).iterator();
            while (iter.hasNext()) {
                StandardFaunusEdge edge = iter.next();
                if (edge.isNew()) iter.remove();
                edge.setLifeCycle(ElementState.DELETED);
            }
        }
    }

    public void removeEdges(final Tokens.Action action, final Direction direction, final String... stringLabels) {
        Set<HadoopType> labels = Sets.newHashSet();
        for (final String label : stringLabels) labels.add(HadoopType.DEFAULT_MANAGER.get(label));

        if (action.equals(Tokens.Action.KEEP)) {
            for (Direction dir : Direction.proper) {
                if (direction == BOTH || direction == dir) {
                    ListMultimap<HadoopType, StandardFaunusEdge> adj = getAdjacency(dir);
                    if (labels.size() > 0) {
                        Set<HadoopType> removal = Sets.newHashSet(adj.keySet());
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

    private class EdgeList extends AbstractList<StandardFaunusEdge> {

        final List<List<StandardFaunusEdge>> edges;

        int fullsize;
        int size;

        public EdgeList(final List<List<StandardFaunusEdge>> edgeLists) {
            this.edges = edgeLists;
            fullsize = 0;
            size = 0;
            for (final List<StandardFaunusEdge> temp : this.edges) {
                fullsize += temp.size();
                for (StandardFaunusEdge e : temp) if (!e.isRemoved()) size++;
            }
        }

        public int size() {
            return this.size;
        }

        public StandardFaunusEdge get(final int index) {
            throw new UnsupportedOperationException();
        }

        public StandardFaunusEdge getDirect(final int index) {
            int lowIndex = 0;
            int highIndex = 0;
            for (final List<StandardFaunusEdge> temp : this.edges) {
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
            for (final List<StandardFaunusEdge> temp : this.edges) {
                highIndex = highIndex + temp.size();
                if (index < highIndex) {
                    temp.remove(index - lowIndex);
                    return;
                }
                lowIndex = lowIndex + temp.size();

            }
            throw new ArrayIndexOutOfBoundsException(index);
        }

        public Iterator<StandardFaunusEdge> iterator() {
            return new Iterator<StandardFaunusEdge>() {
                private int current = -1;
                private int next = findNext(current);

                private int findNext(int current) {
                    int next = current + 1;
                    while (next < fullsize && getDirect(next).isRemoved()) next++;
                    return next;
                }

                @Override
                public boolean hasNext() {
                    return this.next < fullsize;
                }

                @Override
                public StandardFaunusEdge next() {
                    current = next;
                    next = findNext(current);
                    return getDirect(current);
                }

                @Override
                public void remove() {
                    StandardFaunusEdge toDelete = getDirect(current);
                    if (toDelete.isNew()) {
                        removeList(current);
                        next--;
                        fullsize--;
                    }
                    toDelete.setLifeCycle(ElementState.DELETED);
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
        new HadoopSerializer(this.getConf()).writeVertex(this, out);
    }

    public void readFields(final DataInput in) throws IOException {
        new HadoopSerializer(this.getConf()).readVertex(this, in);
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
