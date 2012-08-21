package com.thinkaurelius.faunus;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Query;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.DefaultQuery;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.tinkerpop.blueprints.Direction.BOTH;
import static com.tinkerpop.blueprints.Direction.IN;
import static com.tinkerpop.blueprints.Direction.OUT;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class FaunusVertex extends FaunusElement implements Vertex, WritableComparable<FaunusVertex> {

    static {
        WritableComparator.define(FaunusVertex.class, new Comparator());
    }

    private Map<String, List<Edge>> outEdges = new HashMap<String, List<Edge>>();
    private Map<String, List<Edge>> inEdges = new HashMap<String, List<Edge>>();

    public FaunusVertex() {
        super(-1l);
    }

    public FaunusVertex(final long id) {
        super(id);
    }

    public FaunusVertex(final DataInput in) throws IOException {
        super(-1l);
        this.readFields(in);
    }

    public Query query() {
        return new DefaultQuery(this);
    }

    public Set<String> getEdgeLabels(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return this.outEdges.keySet();
        else if (direction.equals(Direction.IN))
            return this.inEdges.keySet();
        else {
            final Set<String> labels = new HashSet<String>();
            labels.addAll(this.outEdges.keySet());
            labels.addAll(this.inEdges.keySet());
            return labels;
        }
    }

    public Iterable<Vertex> getVertices(final Direction direction, final String... labels) {
        return new Iterable<Vertex>() {
            public Iterator<Vertex> iterator() {
                return new Iterator<Vertex>() {
                    final Iterator<Edge> edges = getEdges(direction).iterator();
                    final Direction opposite = direction.opposite();

                    public void remove() {
                        throw new UnsupportedOperationException();
                    }

                    public boolean hasNext() {
                        return this.edges.hasNext();
                    }

                    public Vertex next() {
                        return this.edges.next().getVertex(opposite);
                    }
                };
            }
        };
    }

    public Iterable<Edge> getEdges(final Direction direction, final String... labels) {
        final List<List<Edge>> edges = new ArrayList<List<Edge>>();
        if (direction.equals(OUT) || direction.equals(BOTH)) {
            if (null == labels || labels.length == 0) {
                for (final List<Edge> temp : this.outEdges.values()) {
                    edges.add(temp);
                }
            } else {
                for (final String label : labels) {
                    final List<Edge> temp = this.outEdges.get(label);
                    if (null != temp)
                        edges.add(temp);
                }
            }
        }

        if (direction.equals(IN) || direction.equals(BOTH)) {
            if (null == labels || labels.length == 0) {
                for (final List<Edge> temp : this.inEdges.values()) {
                    edges.add(temp);
                }
            } else {
                for (final String label : labels) {
                    final List<Edge> temp = this.inEdges.get(label);
                    if (null != temp)
                        edges.add(temp);
                }
            }
        }
        return new EdgeList(edges);
    }

    public FaunusEdge addEdge(final Direction direction, final FaunusEdge edge) {
        if (OUT.equals(direction)) {
            List<Edge> edges = this.outEdges.get(edge.getLabel());
            if (null == edges) {
                edges = new ArrayList<Edge>();
                this.outEdges.put(edge.getLabel(), edges);
            }
            edges.add(edge);
        } else if (IN.equals(direction)) {
            List<Edge> edges = this.inEdges.get(edge.getLabel());
            if (null == edges) {
                edges = new ArrayList<Edge>();
                this.inEdges.put(edge.getLabel(), edges);
            }
            edges.add(edge);
        } else
            throw ExceptionFactory.bothIsNotSupported();

        return edge;
    }

    public void removeEdgesToFrom(final Set<Long> ids) {
        Iterator<Edge> itty = this.getEdges(OUT).iterator();
        while (itty.hasNext()) {
            final Long id = (Long) itty.next().getVertex(IN).getId();
            if (ids.contains(id))
                itty.remove();
        }

        itty = this.getEdges(IN).iterator();
        while (itty.hasNext()) {
            final Long id = (Long) itty.next().getVertex(OUT).getId();
            if (ids.contains(id))
                itty.remove();
        }
    }

    public void removeEdges(final Tokens.Action action, final Direction direction, final String... labels) {
        if (action.equals(Tokens.Action.KEEP)) {
            final Set<String> keep = new HashSet<String>(Arrays.asList(labels));
            if (direction.equals(BOTH) || direction.equals(OUT)) {
                if (labels.length > 0) {
                    for (final String label : new ArrayList<String>(this.outEdges.keySet())) {
                        if (!keep.contains(label))
                            this.outEdges.remove(label);
                    }
                }
            }

            if (direction.equals(BOTH) || direction.equals(IN)) {
                if (labels.length > 0) {
                    for (final String label : new ArrayList<String>(this.inEdges.keySet())) {
                        if (!keep.contains(label))
                            this.inEdges.remove(label);
                    }
                }
            }
        } else {
            if (direction.equals(BOTH) || direction.equals(OUT)) {
                if (labels.length == 0) {
                    this.outEdges.clear();
                } else {
                    for (final String label : labels) {
                        this.outEdges.remove(label);
                    }
                }
            }

            if (direction.equals(BOTH) || direction.equals(IN)) {
                if (labels.length == 0) {
                    this.inEdges.clear();
                } else {
                    for (final String label : labels) {
                        this.inEdges.remove(label);
                    }
                }
            }
        }
    }

    public void write(final DataOutput out) throws IOException {
        out.writeLong(this.id);
        EdgeMap.write((Map) this.inEdges, out, Direction.OUT);
        EdgeMap.write((Map) this.outEdges, out, Direction.IN);
        ElementProperties.write(this.properties, out);

    }

    public void readFields(final DataInput in) throws IOException {
        this.id = in.readLong();
        this.inEdges = (Map) EdgeMap.readFields(in, Direction.OUT, this.id);
        this.outEdges = (Map) EdgeMap.readFields(in, Direction.IN, this.id);
        this.properties = ElementProperties.readFields(in);
    }

    public int compareTo(final FaunusVertex other) {
        return new Long(this.id).compareTo((Long) other.getId());
    }

    public String toString() {
        return StringFactory.vertexString(this);
    }

    public FaunusVertex cloneIdAndProperties() {
        final FaunusVertex clone = new FaunusVertex(this.getIdAsLong());
        clone.setProperties(this.getProperties());
        return clone;
    }

    public FaunusVertex cloneId() {
        return new FaunusVertex(this.getIdAsLong());
    }

    public static class Comparator extends WritableComparator {
        public Comparator() {
            super(FaunusVertex.class);
        }

        @Override
        public int compare(final byte[] vertex1, final int start1, final int length1, final byte[] vertex2, final int start2, final int length2) {
            // the first 8 bytes are the long id
            final ByteBuffer buffer1 = ByteBuffer.wrap(vertex1);
            final ByteBuffer buffer2 = ByteBuffer.wrap(vertex2);
            return (((Long) buffer1.getLong()).compareTo(buffer2.getLong()));
        }

        /* @Override
       public int compare(final WritableComparable a, final WritableComparable b) {
           if (a instanceof FaunusVertex && b instanceof FaunusVertex)
               return  ((Long)(((FaunusVertex) a).getIdAsLong())).compareTo(((FaunusVertex) b).getIdAsLong());
           else
               return super.compare(a, b);
       } */
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

    private static class EdgeMap {
        public static Map<String, List<FaunusEdge>> readFields(final DataInput in, final Direction idToRead, final long otherId) throws IOException {
            final Map<String, List<FaunusEdge>> edges = new HashMap<String, List<FaunusEdge>>();
            int edgeTypes = in.readShort();
            for (int i = 0; i < edgeTypes; i++) {
                final String label = in.readUTF();
                final int size = in.readInt();
                final List<FaunusEdge> temp = new ArrayList<FaunusEdge>(size);
                for (int j = 0; j < size; j++) {
                    final FaunusEdge edge = new FaunusEdge();
                    edge.readFieldsCompressed(in, idToRead);
                    edge.label = label;
                    if (idToRead.equals(Direction.OUT))
                        edge.inVertex = otherId;
                    else
                        edge.outVertex = otherId;
                    temp.add(edge);
                }
                edges.put(label, temp);
            }
            return edges;
        }

        public static void write(final Map<String, List<FaunusEdge>> edges, final DataOutput out, final Direction idToWrite) throws IOException {
            out.writeShort(edges.size());
            for (final Map.Entry<String, List<FaunusEdge>> entry : edges.entrySet()) {
                out.writeUTF(entry.getKey());
                out.writeInt(entry.getValue().size());
                for (final FaunusEdge edge : entry.getValue()) {
                    edge.writeCompressed(out, idToWrite);
                }
            }
        }
    }
}
