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
public class HadoopVertex extends HadoopPathElement implements Vertex {

    protected ListMultimap<HadoopType, HadoopEdge> outEdges = ArrayListMultimap.create();
    protected ListMultimap<HadoopType, HadoopEdge> inEdges = ArrayListMultimap.create();

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
        this.id = vertex.getIdAsLong();
        this.properties = vertex.properties;
        this.getPaths(vertex, false);
        this.state = vertex.getState();
        this.addEdges(BOTH, vertex);
    }

    @Override
    void updateSchema(final HadoopSerializer.Schema schema) {
        super.updateSchema(schema);
        for (Direction dir : Direction.proper) {
            for (HadoopEdge edge : getAdjacency(dir).values())
                edge.updateSchema(schema);
        }
    }

    //##################################
    // Property Handling
    //##################################

    @Override
    public HadoopProperty addProperty(HadoopProperty property) {
        return super.addProperty(property);
    }

    public HadoopProperty addProperty(final String key, final Object value) {
        HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        return addProperty(new HadoopProperty(type, value));
    }

    public <T> Iterable<T> getProperties(final String key) {
        HadoopType type = HadoopType.DEFAULT_MANAGER.get(key);
        if (type.isImplicit()) return Arrays.<T>asList((T)this.getImplicitProperty(type)); // TODO: is this okay?
        return Iterables.transform(Iterables.filter(properties.get(type), FILTER_DELETED_PROPERTIES), new Function<HadoopProperty, T>() {
            @Nullable
            @Override
            public T apply(@Nullable HadoopProperty faunusProperty) {
                return (T) faunusProperty.getValue();
            }
        });
    }

    public Iterable<HadoopProperty> getProperties(final HadoopType type) {
        Preconditions.checkArgument(!type.isImplicit());
        return Iterables.filter(properties.get(type), FILTER_DELETED_PROPERTIES);
    }

    //##################################
    // Edge Handling
    //##################################

    public VertexQuery query() {
        return new DefaultVertexQuery(this);
    }

    private ListMultimap<HadoopType, HadoopEdge> getAdjacency(final Direction direction) {
        switch (direction) {
            case IN:
                return inEdges;
            case OUT:
                return outEdges;
            default:
                throw ExceptionFactory.bothIsNotSupported();
        }
    }

    static boolean containsUndeletedEdge(final ListMultimap<HadoopType, HadoopEdge> edgeList, final HadoopType type) {
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

    public Iterable<HadoopEdge> getEdgesWithState(final Direction direction) {
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
        final List<List<HadoopEdge>> edgeLists = new ArrayList<List<HadoopEdge>>();

        for (final Direction dir : Direction.proper) {
            if (direction != BOTH && direction != dir) continue;
            ListMultimap<HadoopType, HadoopEdge> adj = getAdjacency(dir);
            if (null == labels || labels.length == 0) {
                for (HadoopType type : adj.keySet()) {
                    if (!type.isHidden()) edgeLists.add((List) adj.get(type));
                }
            } else {
                for (final HadoopType label : labels) {
                    final List<HadoopEdge> temp = adj.get(label);
                    edgeLists.add(temp);
                }
            }
        }
        return (Iterable) new EdgeList(edgeLists);
    }


    private void addEdges(final Direction direction, final HadoopType label, final List<HadoopEdge> edges) {
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
        return this.addEdge(Direction.OUT, new HadoopEdge(this.configuration, this.getIdAsLong(), ((HadoopVertex) inVertex).getIdAsLong(), label));
    }

    public Edge addEdge(final Direction direction, final String label, final long otherVertexId) {
        if (direction == OUT)
            return this.addEdge(OUT, new HadoopEdge(this.configuration, this.id, otherVertexId, label));
        else if (direction == Direction.IN)
            return this.addEdge(Direction.IN, new HadoopEdge(this.configuration, otherVertexId, this.id, label));
        else
            throw ExceptionFactory.bothIsNotSupported();
    }

    public HadoopEdge addEdge(final Direction direction, final HadoopEdge edge) {
        edge.setConf(this.getConf());
        getAdjacency(direction).put(edge.getType(), edge);
        return edge;
    }

    public void removeEdgesToFrom(final Set<Long> ids) {
        for (final Direction dir : Direction.proper) {
            Iterator<HadoopEdge> edges = getAdjacency(dir).values().iterator();
            while (edges.hasNext()) {
                HadoopEdge edge = edges.next();
                if (ids.contains(edge.getVertexId(dir.opposite()))) {
                    if (edge.isNew()) edges.remove();
                    edge.setState(ElementState.DELETED);
                }
            }
        }
    }

    private void removeAllEdges(final Direction dir, Iterable<HadoopType> types) {
        types = Lists.newArrayList(types);
        ListMultimap<HadoopType, HadoopEdge> adj = getAdjacency(dir);
        for (HadoopType type : types) {
            Iterator<HadoopEdge> iter = adj.get(type).iterator();
            while (iter.hasNext()) {
                HadoopEdge edge = iter.next();
                if (edge.isNew()) iter.remove();
                edge.setState(ElementState.DELETED);
            }
        }
    }

    public void removeEdges(final Tokens.Action action, final Direction direction, final String... stringLabels) {
        Set<HadoopType> labels = Sets.newHashSet();
        for (final String label : stringLabels) labels.add(HadoopType.DEFAULT_MANAGER.get(label));

        if (action.equals(Tokens.Action.KEEP)) {
            for (Direction dir : Direction.proper) {
                if (direction == BOTH || direction == dir) {
                    ListMultimap<HadoopType, HadoopEdge> adj = getAdjacency(dir);
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

    private class EdgeList extends AbstractList<HadoopEdge> {

        final List<List<HadoopEdge>> edges;

        int fullsize;
        int size;

        public EdgeList(final List<List<HadoopEdge>> edgeLists) {
            this.edges = edgeLists;
            fullsize = 0;
            size = 0;
            for (final List<HadoopEdge> temp : this.edges) {
                fullsize += temp.size();
                for (HadoopEdge e : temp) if (!e.isDeleted()) size++;
            }
        }

        public int size() {
            return this.size;
        }

        public HadoopEdge get(final int index) {
            throw new UnsupportedOperationException();
        }

        public HadoopEdge getDirect(final int index) {
            int lowIndex = 0;
            int highIndex = 0;
            for (final List<HadoopEdge> temp : this.edges) {
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
            for (final List<HadoopEdge> temp : this.edges) {
                highIndex = highIndex + temp.size();
                if (index < highIndex) {
                    temp.remove(index - lowIndex);
                    return;
                }
                lowIndex = lowIndex + temp.size();

            }
            throw new ArrayIndexOutOfBoundsException(index);
        }

        public Iterator<HadoopEdge> iterator() {
            return new Iterator<HadoopEdge>() {
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
                public HadoopEdge next() {
                    current = next;
                    next = findNext(current);
                    return getDirect(current);
                }

                @Override
                public void remove() {
                    HadoopEdge toDelete = getDirect(current);
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
