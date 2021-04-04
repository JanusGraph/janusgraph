// Copyright 2021 JanusGraph Authors
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

package org.janusgraph.graphdb.olap;

import org.janusgraph.core.JanusGraph;
import org.janusgraph.diskstorage.EntryList;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanJob;
import org.janusgraph.graphdb.database.StandardJanusGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.relations.RelationCache;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.system.BaseKey;

public abstract class AbstractScanJob implements ScanJob {
    protected final GraphProvider graph;
    protected StandardJanusGraphTx tx;
    private IDManager idManager;

    public AbstractScanJob(JanusGraph graph) {
        this.graph = new GraphProvider();
        if (graph != null) this.graph.setGraph(graph);
    }

    public AbstractScanJob(AbstractScanJob copy) {
        this.graph = copy.graph;
        this.tx = copy.tx;
        this.idManager = copy.idManager;
    }

    protected abstract StandardJanusGraphTx startTransaction(StandardJanusGraph graph);

    protected void open(Configuration graphConfig) {
        graph.initializeGraph(graphConfig);
        idManager = graph.get().getIDManager();
        tx = startTransaction(graph.get());
    }

    protected void close() {
        if (null != tx && tx.isOpen())
            tx.rollback();
        graph.close();
    }

    protected boolean isGhostVertex(long vertexId, EntryList firstEntries) {
        if (idManager.isPartitionedVertex(vertexId) && !idManager.isCanonicalVertexId(vertexId)) return false;

        RelationCache relCache = tx.getEdgeSerializer().parseRelation(
            firstEntries.get(0), true, tx);
        return relCache.typeId != BaseKey.VertexExists.longId();
    }

    protected long getVertexId(StaticBuffer key) {
        return idManager.getKeyID(key);
    }

    @Override
    public abstract AbstractScanJob clone();
}
