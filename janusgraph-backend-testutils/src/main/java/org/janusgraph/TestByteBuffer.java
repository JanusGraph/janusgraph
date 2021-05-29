// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph;

import com.carrotsearch.hppc.LongObjectHashMap;
import com.carrotsearch.hppc.LongObjectMap;
import org.apache.commons.math.stat.descriptive.SummaryStatistics;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.SortedSet;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class TestByteBuffer {

    private static final int NUM = 1000;
    private static final double FRACTION = 0.2;
    private static final int ROUNDSIZE = 5;
    private static final int TRIALS = 5;
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
                    Edge e = new Edge(vertices[i], vertices[j]);
                    e.setProperty(random.nextInt(ROUNDSIZE));
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
        final LongObjectMap<ConcurrentSkipListSet<ByteEntry>> tx = new LongObjectHashMap<>(NUM);
        for (int i = 0; i < NUM; i++) {
            tx.put(i, new ConcurrentSkipListSet<>());
        }
        for (int i = 0; i < NUM; i++) {
            for (int j = 0; j < NUM; j++) {
                if (i == j) continue;
                if (Math.random() < FRACTION) {
                    ByteBuffer key = ByteBuffer.allocate(16);
                    key.putLong(5).putLong(j).flip();
                    ByteBuffer value = ByteBuffer.allocate(4);
                    value.putInt(random.nextInt(ROUNDSIZE)).flip();
                    tx.get(i).add(new ByteEntry(key, value));
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

    abstract static class Vertex implements Comparable<Vertex> {

        protected final long id;

        Vertex(long id) {
            this.id = id;
        }

        @Override
        public int compareTo(Vertex vertex) {
            return Long.compare(id, vertex.id);
        }

        public long getId() {
            return id;
        }

        public abstract Iterable<Vertex> getNeighbors(int value);
    }

    static class EdgeVertex extends Vertex {

        private final SortedSet<Edge> outEdges = new ConcurrentSkipListSet<>(Comparator.comparing(Edge::getEnd));

        EdgeVertex(long id) {
            super(id);
        }

        @Override
        public Iterable<Vertex> getNeighbors(final int value) {
            return outEdges.stream()
                .filter(edge -> (Integer) edge.getProperty("number") == value)
                .map(Edge::getEnd)
                .collect(Collectors.toSet());
        }

        void addOutEdge(Edge e) {
            outEdges.add(e);
        }
    }

    static class ByteVertex extends Vertex {

        private final LongObjectMap<ConcurrentSkipListSet<ByteEntry>> tx;
        private final SortedSet<ByteEntry> set;

        ByteVertex(long id, LongObjectMap<ConcurrentSkipListSet<ByteEntry>> tx) {
            super(id);
            this.tx = tx;
            this.set = tx.get(id);
        }

        @Override
        public Iterable<Vertex> getNeighbors(final int value) {
//            SortedSet<ByteEntry> set = (SortedSet<ByteEntry>) tx.get(id);
            return set.stream()
                .filter(entry -> entry.value.getInt(0) == value)
                .map(entry -> new ByteVertex(entry.key.getLong(8), tx))
                .collect(Collectors.toSet());
        }
    }


    static class Edge {

        private final Vertex start;
        private final Vertex end;
        private final String label;
        private final Map<String, Object> properties = new HashMap<>();

        Edge(Vertex start, Vertex end) {
            this.label = "connect";
            this.end = end;
            this.start = start;
        }

        public String getLabel() {
            return label;
        }

        void setProperty(Object value) {
            properties.put("number", value);
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
