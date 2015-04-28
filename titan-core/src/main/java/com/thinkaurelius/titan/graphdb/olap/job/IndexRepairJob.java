package com.thinkaurelius.titan.graphdb.olap.job;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.schema.*;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.management.RelationTypeIndexWrapper;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.olap.VertexScanJob;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.types.CompositeIndexType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.MixedIndexType;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.*;

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

    public IndexRepairJob() {
        super();
    }

    protected IndexRepairJob(IndexRepairJob job) { super(job); }

    public IndexRepairJob(final String indexName, final String indexType) {
        super(indexName,indexType);
    }

    @Override
    public void workerIterationEnd(ScanMetrics metrics) {
        super.workerIterationEnd(metrics);
    }

    @Override
    public void workerIterationStart(final TitanGraph graph, Configuration config, ScanMetrics metrics) {
        super.workerIterationStart(graph, config, metrics);
    }

    /**
     * Check that our target index is in either the ENABLED or REGISTERED state.
     */
    @Override
    protected void validateIndexStatus() {
        TitanSchemaVertex schemaVertex = mgmt.getSchemaVertex(index);
        Set<SchemaStatus> acceptableStatuses = SchemaAction.REINDEX.getApplicableStatus();
        boolean isValidIndex = true;
        String invalidIndexHint;
        if (index instanceof RelationTypeIndex ||
                (index instanceof TitanGraphIndex && ((TitanGraphIndex)index).isCompositeIndex()) ) {
            SchemaStatus actualStatus = schemaVertex.getStatus();
            isValidIndex = acceptableStatuses.contains(actualStatus);
            invalidIndexHint = String.format(
                    "The index has status %s, but one of %s is required",
                    actualStatus, acceptableStatuses);
        } else {
            Preconditions.checkArgument(index instanceof TitanGraphIndex,"Unexpected index: %s",index);
            TitanGraphIndex gindex = (TitanGraphIndex)index;
            Preconditions.checkArgument(gindex.isMixedIndex());
            Map<String, SchemaStatus> invalidKeyStatuses = new HashMap<>();
            int acceptableFields = 0;
            for (PropertyKey key : gindex.getFieldKeys()) {
                SchemaStatus status = gindex.getIndexStatus(key);
                if (status!=SchemaStatus.DISABLED && !acceptableStatuses.contains(status)) {
                    isValidIndex=false;
                    invalidKeyStatuses.put(key.name(), status);
                    log.warn("Index {} has key {} in an invalid status {}",index,key,status);
                }
                if (acceptableStatuses.contains(status)) acceptableFields++;
            }
            invalidIndexHint = String.format(
                    "The following index keys have invalid status: %s (status must be one of %s)",
                    Joiner.on(",").withKeyValueSeparator(" has status ").join(invalidKeyStatuses), acceptableStatuses);
            if (isValidIndex && acceptableFields==0) {
                isValidIndex = false;
                invalidIndexHint = "The index does not contain any valid keys";
            }
        }
        Preconditions.checkArgument(isValidIndex, "The index %s is in an invalid state and cannot be indexed. %s", indexName, invalidIndexHint);
        // TODO consider retrieving the current Job object and calling killJob() if !isValidIndex -- would be more efficient than throwing an exception on the first pair processed by each mapper
        log.debug("Index {} is valid for re-indexing");
    }


    @Override
    public void process(TitanVertex vertex, ScanMetrics metrics) {
        try {
            BackendTransaction mutator = writeTx.getTxHandle();
            if (index instanceof RelationTypeIndex) {
                RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper)index;
                InternalRelationType wrappedType = wrapper.getWrappedType();
                EdgeSerializer edgeSerializer = writeTx.getEdgeSerializer();
                List<Entry> additions = new ArrayList<>();

                for (TitanRelation relation : vertex.query().types(indexRelationTypeName).direction(Direction.OUT).relations()) {
                    InternalRelation titanRelation = (InternalRelation)relation;
                    for (int pos = 0; pos < titanRelation.getArity(); pos++) {
                        if (!wrappedType.isUnidirected(Direction.BOTH) && !wrappedType.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered
                        Entry entry = edgeSerializer.writeRelation(titanRelation, wrappedType, pos, writeTx);
                        additions.add(entry);
                    }
                }
                StaticBuffer vertexKey = writeTx.getIdInspector().getKey(vertex.longId());
                mutator.mutateEdges(vertexKey, additions, KCVSCache.NO_DELETIONS);
                metrics.incrementCustom(ADDED_RECORDS_COUNT, additions.size());
            } else if (index instanceof TitanGraphIndex) {
                IndexType indexType = mgmt.getSchemaVertex(index).asIndexType();
                assert indexType!=null;
                IndexSerializer indexSerializer = graph.getIndexSerializer();
                //Gather elements to index
                List<TitanElement> elements;
                switch (indexType.getElement()) {
                    case VERTEX:
                        elements = ImmutableList.of(vertex);
                        break;
                    case PROPERTY:
                        elements = Lists.newArrayList();
                        for (TitanVertexProperty p : addIndexSchemaConstraint(vertex.query(),indexType).properties()) {
                            elements.add(p);
                        }
                        break;
                    case EDGE:
                        elements = Lists.newArrayList();
                        for (TitanEdge e : addIndexSchemaConstraint(vertex.query().direction(Direction.OUT),indexType).edges()) {
                            elements.add(e);
                        }
                        break;
                    default: throw new AssertionError("Unexpected category: " + indexType.getElement());
                }
                if (indexType.isCompositeIndex()) {
                    for (TitanElement element : elements) {
                        Set<IndexSerializer.IndexUpdate<StaticBuffer,Entry>> updates =
                                indexSerializer.reindexElement(element, (CompositeIndexType) indexType);
                        for (IndexSerializer.IndexUpdate<StaticBuffer,Entry> update : updates) {
                            log.debug("Mutating index {}: {}", indexType, update.getEntry());
                            mutator.mutateIndex(update.getKey(), Lists.newArrayList(update.getEntry()), KCVSCache.NO_DELETIONS);
                            metrics.incrementCustom(ADDED_RECORDS_COUNT);
                        }
                    }
                } else {
                    assert indexType.isMixedIndex();
                    Map<String,Map<String,List<IndexEntry>>> documentsPerStore = new HashMap<>();
                    for (TitanElement element : elements) {
                        indexSerializer.reindexElement(element, (MixedIndexType) indexType, documentsPerStore);
                        metrics.incrementCustom(DOCUMENT_UPDATES_COUNT);
                    }
                    mutator.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
                }

            } else throw new UnsupportedOperationException("Unsupported index found: "+index);
        } catch (final Exception e) {
            mgmt.rollback();
            writeTx.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new TitanException(e.getMessage(), e);
        }
    }

    @Override
    public void getQueries(QueryContainer queries) {
        if (index instanceof RelationTypeIndex) {
            queries.addQuery().types(indexRelationTypeName).direction(Direction.OUT).relations();
        } else if (index instanceof TitanGraphIndex) {
            IndexType indexType = mgmt.getSchemaVertex(index).asIndexType();
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
            TitanSchemaType constraint = indexType.getSchemaTypeConstraint();
            Preconditions.checkArgument(constraint instanceof RelationType,"Expected constraint to be a " +
                    "relation type: %s",constraint);
            query.types((RelationType)constraint);
        }
        return query;
    }
}
