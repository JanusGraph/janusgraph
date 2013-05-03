package com.thinkaurelius.titan;

import cern.colt.map.AbstractLongObjectMap;
import cern.colt.map.OpenLongObjectHashMap;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import javax.annotation.Nullable;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestByteBuffer {

    private static final int NUM = 1000;
    private static final double FRACTION = 0.2;
    private static final int ROUNDSIZE = 5;
    private static final int TRIALS = 5;
    private static final boolean CHECK_VALUE = true;

    private static final Random random = new Random();

    public static void main(String[] args) {
        SummaryStatistics statObject = new SummaryStatistics();
        SummaryStatistics statByte = new SummaryStatistics();
        for (int i = 0; i < 10; i++) {
            statByte.addValue(testByte());
            statObject.addValue(testObject());
        }
        System.out.println("Time (ms) Object: " + statObject.getMean() + " | " + statObject.getStandardDeviation());
        System.out.println("Time (ms) Byte: " + statByte.getMean() + " | " + statByte.getStandardDeviation());
    }

    private static long testObject() {
        EdgeVertex[] vertices = new EdgeVertex[NUM];
        for (int i = 0; i < NUM; i++) {
            vertices[i] = new EdgeVertex(i);
        }
        for (int i = 0; i < NUM; i++) {
            for (int j = 0; j < NUM; j++) {
                if (i == j) continue;
                if (Math.random() < FRACTION) {
                    Edge e = new Edge(vertices[i], "connect", vertices[j]);
                    e.setProperty("number", random.nextInt(ROUNDSIZE));
                    vertices[i].addOutEdge(e);
                }
            }
        }
        long time = System.currentTimeMillis();
        long sum = 0;
        for (int t = 0; t < TRIALS; t++) {
            for (int i = 0; i < NUM; i++) {
                for (Vertex v : vertices[i].getNeighbors(0)) {
                    sum += v.getId();
                }
            }
        }
        time = System.currentTimeMillis() - time;
        return time;
    }

    private static long testByte() {
        AbstractLongObjectMap tx = new OpenLongObjectHashMap(NUM);
        for (int i = 0; i < NUM; i++) {
            tx.put(i, new ConcurrentSkipListSet<ByteEntry>());
        }
        for (int i = 0; i < NUM; i++) {
            for (int j = 0; j < NUM; j++) {
                if (i == j) continue;
                if (Math.random() < FRACTION) {
                    ByteBuffer key = ByteBuffer.allocate(16);
                    key.putLong(5).putLong(j).flip();
                    ByteBuffer value = ByteBuffer.allocate(4);
                    value.putInt(random.nextInt(ROUNDSIZE)).flip();
                    ((ConcurrentSkipListSet<ByteEntry>) tx.get(i)).add(new ByteEntry(key, value));
                }
            }
        }
        long time = System.currentTimeMillis();
        long sum = 0;
        for (int t = 0; t < TRIALS; t++) {
            for (int i = 0; i < NUM; i++) {
                for (Vertex v : (new ByteVertex(i, tx)).getNeighbors(0)) {
                    sum += v.getId();
                }
            }
        }
        time = System.currentTimeMillis() - time;
        return time;
    }

    static abstract class Vertex implements Comparable<Vertex> {

        protected final long id;

        Vertex(long id) {
            this.id = id;
        }

        @Override
        public int compareTo(Vertex vertex) {
            return Long.valueOf(id).compareTo(vertex.id);
        }

        public long getId() {
            return id;
        }

        public abstract Iterable<Vertex> getNeighbors(int value);
    }

    static class EdgeVertex extends Vertex {

        private SortedSet<Edge> outEdges = new ConcurrentSkipListSet<Edge>(new Comparator<Edge>() {
            @Override
            public int compare(Edge e1, Edge e2) {
                return e1.getEnd().compareTo(e2.getEnd());
            }
        });

        EdgeVertex(long id) {
            super(id);
        }

        @Override
        public Iterable<Vertex> getNeighbors(final int value) {
            return Iterables.transform(Iterables.filter(outEdges, new Predicate<Edge>() {
                @Override
                public boolean apply(@Nullable Edge edge) {
                    return !CHECK_VALUE || ((Integer) edge.getProperty("number")).intValue() == value;
                }
            }), new Function<Edge, Vertex>() {
                @Override
                public Vertex apply(@Nullable Edge edge) {
                    return edge.getEnd();
                }
            });
        }

        void addOutEdge(Edge e) {
            outEdges.add(e);
        }
    }

    static class ByteVertex extends Vertex {

        private final AbstractLongObjectMap tx;
        private final SortedSet<ByteEntry> set;

        ByteVertex(long id, AbstractLongObjectMap tx) {
            super(id);
            this.tx = tx;
            this.set = (SortedSet<ByteEntry>) tx.get(id);
        }

        @Override
        public Iterable<Vertex> getNeighbors(final int value) {
//            SortedSet<ByteEntry> set = (SortedSet<ByteEntry>) tx.get(id);
            return Iterables.transform(Iterables.filter(set, new Predicate<ByteEntry>() {
                @Override
                public boolean apply(@Nullable ByteEntry entry) {
                    return !CHECK_VALUE || entry.value.getInt(0) == value;
                }
            }), new Function<ByteEntry, Vertex>() {
                @Override
                public Vertex apply(@Nullable ByteEntry entry) {
                    return new ByteVertex(entry.key.getLong(8), tx);
                }
            });
        }
    }


    static class Edge {

        private final Vertex start;
        private final Vertex end;
        private final String label;
        private final Map<String, Object> properties = new HashMap<String, Object>();

        Edge(Vertex start, String label, Vertex end) {
            this.label = label;
            this.end = end;
            this.start = start;
        }

        public String getLabel() {
            return label;
        }

        void setProperty(String key, Object value) {
            properties.put(key, value);
        }

        public Object getProperty(String key) {
            return properties.get(key);
        }

        public Vertex getStart() {
            return start;
        }

        public Vertex getEnd() {
            return end;
        }

        public Vertex getOther(Vertex v) {
            if (start.equals(v)) return end;
            else if (end.equals(v)) return start;
            throw new IllegalArgumentException();
        }
    }

    static class ByteEntry implements Comparable<ByteEntry> {
        final ByteBuffer key;
        final ByteBuffer value;


        ByteEntry(ByteBuffer key, ByteBuffer value) {
            this.value = value;
            this.key = key;
        }

        @Override
        public int compareTo(ByteEntry byteEntry) {
            return key.compareTo(byteEntry.key);
        }
    }

}
