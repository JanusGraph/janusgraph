// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.graphdb

import com.google.common.base.Preconditions
import org.janusgraph.core.Cardinality
import org.janusgraph.core.PropertyKey
import org.janusgraph.core.JanusGraph
import org.janusgraph.core.schema.ConsistencyModifier
import org.janusgraph.core.schema.JanusGraphManagement
import org.janusgraph.graphdb.types.StandardEdgeLabelMaker
import org.apache.tinkerpop.gremlin.structure.Edge
import org.apache.tinkerpop.gremlin.structure.Vertex
import org.slf4j.Logger
import org.slf4j.LoggerFactory

class SpeedTestSchema {

    // Graph element counts
    public static final int VERTEX_COUNT = 10 * 100
    public static final int EDGE_COUNT = VERTEX_COUNT * 5

    public static final String VERTEX_KEY_PREFIX = "vp_"
    public static final String EDGE_KEY_PREFIX = "ep_"
    public static final String LABEL_PREFIX = "el_"
    public static final String UID_PROP = "uid"

    public static long SUPERNODE_UID = 0L
    private static final int SUPERNODE_INDEX = 0

    private final int edgeCount = EDGE_COUNT
    private final int vertexCount = VERTEX_COUNT
    private final int maxEdgePropVal = 100
    private final int maxVertexPropVal = 100
    /*
     * edgeCount must have type int instead of long because
     * DistributionGenerator expects int. It's probably not a great idea to go
     * over 4B per label in memory anyway.
     */
    private final int vertexPropKeys = 20
    private final int edgePropKeys = 10
    private final int edgeLabels = 3

    private final String[] vertexPropNames
    private final String[] edgePropNames
    private final String[] edgeLabelNames
    private final Map<String, String> labelPkeys

    private static final Logger log =
            LoggerFactory.getLogger(SpeedTestSchema.class)


    /*
     * This builder is a relic from back when GraphGenerator existed as
     * a counterpart to this class, and the graph generation parameters
     * were potentially configurable.  Leaving this in until tests pass
     * on 0.9.0 again.
     */
//    public static class Builder {
//
//        private int maxVertexPropVal = 100;
//        private int maxEdgePropVal = 100;
//        private int vertexPropKeys = 20;
//        private int edgePropKeys = 10;
//        private int edgeLabels = 3;
//        private int vertexCount = -1;
//        private int edgeCount = -1;
//
//        /**
//         * Set the maximum value of vertex properties. This is an exclusive
//         * limit. The minimum is always 0.
//         *
//         * @param m maximum vertex property value, exclusive
//         * @return self
//         */
//        public Builder setMaxVertexPropVal(int m) {
//            maxVertexPropVal = m;
//            return this;
//        }
//
//        /**
//         * Set the maximum value of edge properties. This is an exclusive limit.
//         * The minimum is always 0.
//         *
//         * @param m maximum edge property value, exclusive
//         * @return self
//         */
//        public Builder setMaxEdgePropVal(int m) {
//            maxEdgePropVal = m;
//            return this;
//        }
//
//        /**
//         * Set the total number of distinct property keys to use for vertex
//         * properties.
//         *
//         * @param vertexPropKeys number of property keys
//         * @return self
//         */
//        public Builder setVertexPropKeys(int vertexPropKeys) {
//            this.vertexPropKeys = vertexPropKeys;
//            return this;
//        }
//
//        /**
//         * Set the total number of distinct property keys to use for edge
//         * properties.
//         *
//         * @param edgePropKeys number of property keys
//         * @return self
//         */
//        public Builder setEdgePropKeys(int edgePropKeys) {
//            this.edgePropKeys = edgePropKeys;
//            return this;
//        }
//
//        /**
//         * Set the total number of edge labels to create.
//         *
//         * @param edgeLabels number of edge labels
//         * @return self
//         */
//        public Builder setEdgeLabels(int edgeLabels) {
//            this.edgeLabels = edgeLabels;
//            return this;
//        }
//
//        /**
//         * Set the number of vertices to create.
//         *
//         * @param vertexCount global vertex total
//         * @return self
//         */
//        public Builder setVertexCount(int vertexCount) {
//            this.vertexCount = vertexCount;
//            Preconditions.checkArgument(0 <= this.vertexCount);
//            return this;
//        }
//
//        /**
//         * Set the number of edges to create for each edge label.
//         *
//         * @param edgeCount global edge total for each label
//         * @return self
//         */
//        public Builder setEdgeCount(int edgeCount) {
//            this.edgeCount = edgeCount;
//            Preconditions.checkArgument(0 <= this.edgeCount);
//            return this;
//        }
//
//        public Builder(int vertexCount, int edgeCount) {
//            setVertexCount(vertexCount);
//            setEdgeCount(edgeCount);
//        }
//
//        /**
//         * Construct a schema instance with this {@code Builder}'s
//         * settings.
//         *
//         * @return a new GraphGenerator
//         */
//        public SerialSpeedTestSchema build() {
//            return new SerialSpeedTestSchema(maxEdgePropVal, maxVertexPropVal, vertexCount, edgeCount, vertexPropKeys, edgePropKeys, edgeLabels);
//        }
//    }

    static SpeedTestSchema get() {
        return new SpeedTestSchema(VERTEX_COUNT, EDGE_COUNT)
    }

    final String getVertexPropertyName(int i) {
        return vertexPropNames[i]
    }

    final String getEdgePropertyName(int i) {
        return edgePropNames[i]
    }

    final String getEdgeLabelName(int i) {
        return edgeLabelNames[i]
    }

    static final String getSortKeyForLabel(String l) {
        return l.replace("el_", "ep_")
    }

    final int getVertexPropKeys() {
        return vertexPropKeys
    }

    final int getEdgePropKeys() {
        return edgePropKeys
    }

    final int getMaxEdgePropVal() {
        return maxEdgePropVal
    }

    final int getMaxVertexPropVal() {
        return maxVertexPropVal
    }

    final int getEdgeLabels() {
        return edgeLabels
    }

    static final long getSupernodeUid() {
        return SUPERNODE_UID
    }

    final String getSupernodeOutLabel() {
        return getEdgeLabelName(SUPERNODE_INDEX)
    }

    final long getMaxUid() {
        return vertexCount
    }

    final int getVertexCount() {
        return vertexCount
    }

    final int getEdgeCount() {
        return edgeCount
    }

    private SpeedTestSchema() {

        this.vertexPropNames = generateNames(VERTEX_KEY_PREFIX, this.vertexPropKeys)
        this.edgePropNames = generateNames(EDGE_KEY_PREFIX, this.edgePropKeys)
        this.edgeLabelNames = generateNames(LABEL_PREFIX, this.edgeLabels)

        Preconditions.checkArgument(this.edgeLabels <= this.edgePropKeys)

        this.labelPkeys = new HashMap<String, String>(this.edgeLabels)
        for (int i = 0; i < this.edgeLabels; i++) {
            labelPkeys.put(edgeLabelNames[i], edgePropNames[i])
        }
    }


    void makeTypes(JanusGraph g) {
        Preconditions.checkArgument(edgeLabels <= edgePropKeys)

        JanusGraphManagement mgmt = g.openManagement()
        for (int i = 0; i < vertexPropKeys; i++) {
            PropertyKey key = mgmt.makePropertyKey(getVertexPropertyName(i)).dataType(Integer.class).cardinality(Cardinality.SINGLE).make()
            mgmt.setConsistency(key, ConsistencyModifier.LOCK)
            mgmt.buildIndex("v-"+getVertexPropertyName(i),Vertex.class).addKey(key).buildCompositeIndex()
        }
        for (int i = 0; i < edgePropKeys; i++) {
            PropertyKey key = mgmt.makePropertyKey(getEdgePropertyName(i)).dataType(Integer.class).cardinality(Cardinality.SINGLE).make()
            mgmt.setConsistency(key, ConsistencyModifier.LOCK)
            mgmt.buildIndex("e-"+getEdgePropertyName(i),Edge.class).addKey(key).buildCompositeIndex()
        }
        for (int i = 0; i < edgeLabels; i++) {
            String labelName = getEdgeLabelName(i)
            String pkName = getSortKeyForLabel(labelName)
            PropertyKey pk = mgmt.getPropertyKey(pkName)
            ((StandardEdgeLabelMaker)mgmt.makeEdgeLabel(getEdgeLabelName(i))).sortKey(pk).make()
        }

        PropertyKey uid = mgmt.makePropertyKey(UID_PROP).dataType(Long.class).cardinality(Cardinality.SINGLE).make()
        mgmt.buildIndex("v-uid",Vertex.class).unique().addKey(uid).buildCompositeIndex()
        mgmt.setConsistency(uid, ConsistencyModifier.LOCK)
        mgmt.commit()
        log.debug("Committed types")
    }

    private static String[] generateNames(String prefix, int count) {
        String[] result = new String[count]
        StringBuilder sb = new StringBuilder(8)
        sb.append(prefix)
        for (int i = 0; i < count; i++) {
            sb.append(i)
            result[i] = sb.toString()
            sb.setLength(prefix.length())
        }
        return result
    }
}
