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

package org.janusgraph.graphdb.olap.job;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphException;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BackendTransaction;
import org.janusgraph.diskstorage.Entry;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.keycolumnvalue.cache.KCVSCache;
import org.janusgraph.diskstorage.keycolumnvalue.scan.ScanMetrics;
import org.janusgraph.graphdb.database.EdgeSerializer;
import org.janusgraph.graphdb.database.IndexSerializer;
import org.janusgraph.graphdb.database.management.RelationTypeIndexWrapper;
import org.janusgraph.graphdb.internal.InternalRelation;
import org.janusgraph.graphdb.internal.InternalRelationType;
import org.janusgraph.graphdb.internal.InternalVertex;
import org.janusgraph.graphdb.olap.QueryContainer;
import org.janusgraph.graphdb.olap.VertexScanJob;
import org.janusgraph.graphdb.relations.EdgeDirection;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.vertices.JanusGraphSchemaVertex;
import org.janusgraph.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexRepairJob extends IndexUpdateJob implements VertexScanJob {

    /**
     * The number of composite-index entries modified or added to the storage
     * backend by this job.
     */
    public static final String ADDED_RECORDS_COUNT = "adds";

    /**
     * The number of mixed-index documents (or whatever idiom is equivalent to the
     * document in the backend implementation) modified by this job
     */
    public static final String DOCUMENT_UPDATES_COUNT = "doc-updates";

    private Map<String,Map<String,List<IndexEntry>>> documentsPerStore = new HashMap<>();

    public IndexRepairJob() {
        super();
    }

    protected IndexRepairJob(IndexRepairJob job) { super(job); }

    public IndexRepairJob(final String indexName, final String indexType) {
        super(indexName,indexType);
    }

    /**
     * Check that our target index is in either the ENABLED or REGISTERED state.
     */
    @Override
    protected void validateIndexStatus() {
        JanusGraphSchemaVertex schemaVertex = managementSystem.getSchemaVertex(index);
        Set<SchemaStatus> acceptableStatuses = SchemaAction.REINDEX.getApplicableStatus();
        boolean isValidIndex = true;
        String invalidIndexHint;
        if (index instanceof RelationTypeIndex ||
                (index instanceof JanusGraphIndex && ((JanusGraphIndex)index).isCompositeIndex()) ) {
            SchemaStatus actualStatus = schemaVertex.getStatus();
            isValidIndex = acceptableStatuses.contains(actualStatus);
            invalidIndexHint = String.format(
                    "The index has status %s, but one of %s is required",
                    actualStatus, acceptableStatuses);
        } else {
            Preconditions.checkArgument(index instanceof JanusGraphIndex,"Unexpected index: %s",index);
            JanusGraphIndex graphIndex = (JanusGraphIndex)index;
            Preconditions.checkArgument(graphIndex.isMixedIndex());
            Map<String, SchemaStatus> invalidKeyStatuses = new HashMap<>();
            int acceptableFields = 0;
            for (PropertyKey key : graphIndex.getFieldKeys()) {
                SchemaStatus status = graphIndex.getIndexStatus(key);
                if (status!=SchemaStatus.DISABLED && !acceptableStatuses.contains(status)) {
                    isValidIndex=false;
                    invalidKeyStatuses.put(key.name(), status);
                    log.warn("Index {} has key {} in an invalid status {}",index,key,status);
                }
                if (acceptableStatuses.contains(status)) acceptableFields++;
            }
            invalidIndexHint = String.format(
                    "The following index keys have invalid status: %s (status must be one of %s)",
                StringUtils.join(invalidKeyStatuses, " has status ", ","), acceptableStatuses);
            if (isValidIndex && acceptableFields==0) {
                isValidIndex = false;
                invalidIndexHint = "The index does not contain any valid keys";
            }
        }
        Preconditions.checkArgument(isValidIndex, "The index %s is in an invalid state and cannot be indexed. %s", indexName, invalidIndexHint);
        // TODO consider retrieving the current Job object and calling killJob() if !isValidIndex -- would be more efficient than throwing an exception on the first pair processed by each mapper
        if(log.isDebugEnabled()){
            log.debug("Index "+index.name()+" is valid for re-indexing");
        }
    }


    @Override
    public void process(JanusGraphVertex vertex, ScanMetrics metrics) {
        try {
            BackendTransaction mutator = writeTx.getTxHandle();
            if (index instanceof RelationTypeIndex) {
                RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper)index;
                InternalRelationType wrappedType = wrapper.getWrappedType();
                EdgeSerializer edgeSerializer = writeTx.getEdgeSerializer();
                List<Entry> outAdditions = new ArrayList<>();
                Map<StaticBuffer,List<Entry>> inAdditionsMap = new HashMap<>();

                for (Object relation : vertex.query().types(indexRelationTypeName).direction(Direction.OUT).relations()) {
                    InternalRelation janusgraphRelation = (InternalRelation) relation;
                    for (int pos = 0; pos < janusgraphRelation.getArity(); pos++) {
                        if (!wrappedType.isUnidirected(Direction.BOTH) && !wrappedType.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered

                        Entry entry = edgeSerializer.writeRelation(janusgraphRelation, wrappedType, pos, writeTx);

                        if (pos == 0) {
                            outAdditions.add(entry);
                        } else {
                            assert pos == 1;
                            InternalVertex otherVertex = janusgraphRelation.getVertex(1);
                            StaticBuffer otherVertexKey = writeTx.getIdInspector().getKey(otherVertex.longId());
                            inAdditionsMap.computeIfAbsent(otherVertexKey, k -> new ArrayList<>()).add(entry);
                        }
                    }
                }

                //Mutating all OUT relationships for the current vertex
                StaticBuffer vertexKey = writeTx.getIdInspector().getKey(vertex.longId());
                mutator.mutateEdges(vertexKey, outAdditions, KCVSCache.NO_DELETIONS);

                //Mutating all IN relationships for the current vertex
                int totalInAdditions = 0;
                for(Map.Entry<StaticBuffer, List<Entry>> entry : inAdditionsMap.entrySet()) {
                    StaticBuffer otherVertexKey = entry.getKey();
                    List<Entry> inAdditions = entry.getValue();
                    totalInAdditions += inAdditions.size();
                    mutator.mutateEdges(otherVertexKey, inAdditions, KCVSCache.NO_DELETIONS);
                }
                metrics.incrementCustom(ADDED_RECORDS_COUNT, outAdditions.size()+totalInAdditions);
            } else if (index instanceof JanusGraphIndex) {
                IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
                assert indexType!=null;
                IndexSerializer indexSerializer = graph.getIndexSerializer();
                //Gather elements to index
                List<JanusGraphElement> elements;
                switch (indexType.getElement()) {
                    case VERTEX:
                        elements = Collections.singletonList(vertex);
                        break;
                    case PROPERTY:
                        elements = new ArrayList<>();
                        addIndexSchemaConstraint(vertex.query(),indexType).properties().forEach(elements::add);
                        break;
                    case EDGE:
                        elements = new ArrayList<>();
                        addIndexSchemaConstraint(vertex.query().direction(Direction.OUT),indexType).edges().forEach(elements::add);
                        break;
                    default: throw new AssertionError("Unexpected category: " + indexType.getElement());
                }
                if (indexType.isCompositeIndex()) {
                    for (JanusGraphElement element : elements) {
                        Set<IndexSerializer.IndexUpdate<StaticBuffer,Entry>> updates =
                                indexSerializer.reindexElement(element, (CompositeIndexType) indexType);
                        for (IndexSerializer.IndexUpdate<StaticBuffer,Entry> update : updates) {
                            log.debug("Mutating index {}: {}", indexType, update.getEntry());
                            mutator.mutateIndex(update.getKey(), new ArrayList<Entry>(1){{add(update.getEntry());}}, KCVSCache.NO_DELETIONS);
                            metrics.incrementCustom(ADDED_RECORDS_COUNT);
                        }
                    }
                } else {
                    assert indexType.isMixedIndex();
                    for (JanusGraphElement element : elements) {
                        if (indexSerializer.reindexElement(element, (MixedIndexType) indexType, documentsPerStore)) {
                            metrics.incrementCustom(DOCUMENT_UPDATES_COUNT);
                        }
                    }
                }

            } else throw new UnsupportedOperationException("Unsupported index found: "+index);
        } catch (final Exception e) {
            managementSystem.rollback();
            writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new JanusGraphException(e.getMessage(), e);
        }
    }

    @Override
    public void workerIterationEnd(final ScanMetrics metrics) {
        try {
            if (index instanceof JanusGraphIndex) {
                BackendTransaction mutator = writeTx.getTxHandle();
                IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
                if (indexType.isMixedIndex() && documentsPerStore.size() > 0) {
                    mutator.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
                    documentsPerStore = new HashMap<>();
                }
            }
        } catch (BackendException e) {
            throw new JanusGraphException(e.getMessage(), e);
        } finally {
            super.workerIterationEnd(metrics);
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {
        if (index instanceof RelationTypeIndex) {
            queries.addQuery().types(indexRelationTypeName).direction(Direction.OUT).relations();
        } else if (index instanceof JanusGraphIndex) {
            IndexType indexType = managementSystem.getSchemaVertex(index).asIndexType();
            switch (indexType.getElement()) {
                case PROPERTY:
                    addIndexSchemaConstraint(queries.addQuery(),indexType).properties();
                    break;
                case VERTEX:
                    queries.addQuery().properties();
                    queries.addQuery().type(BaseLabel.VertexLabelEdge).direction(Direction.OUT).edges();
                    break;
                case EDGE:
                    indexType.hasSchemaTypeConstraint();
                    addIndexSchemaConstraint(queries.addQuery().direction(Direction.OUT),indexType).edges();
                    break;
                default: throw new AssertionError("Unexpected category: " + indexType.getElement());
            }
        } else throw new UnsupportedOperationException("Unsupported index found: "+index);
    }

    @Override
    public IndexRepairJob clone() {
        return new IndexRepairJob(this);
    }

    private static<Q extends BaseVertexQuery> Q addIndexSchemaConstraint(Q query, IndexType indexType) {
        if (indexType.hasSchemaTypeConstraint()) {
            JanusGraphSchemaType constraint = indexType.getSchemaTypeConstraint();
            Preconditions.checkArgument(constraint instanceof RelationType,"Expected constraint to be a " +
                    "relation type: %s",constraint);
            query.types((RelationType)constraint);
        }
        return query;
    }
}
