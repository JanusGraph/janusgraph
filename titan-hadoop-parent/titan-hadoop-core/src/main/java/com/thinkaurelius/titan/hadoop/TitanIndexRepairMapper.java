package com.thinkaurelius.titan.hadoop;

import static com.thinkaurelius.titan.hadoop.compat.HadoopCompatLoader.DEFAULT_COMPAT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanEdge;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.core.schema.SchemaAction;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.core.schema.TitanIndex;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.diskstorage.indexing.IndexEntry;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.thinkaurelius.titan.graphdb.database.EdgeSerializer;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import com.thinkaurelius.titan.graphdb.database.management.RelationTypeIndexWrapper;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.StandardEdge;
import com.thinkaurelius.titan.graphdb.relations.StandardProperty;
import com.thinkaurelius.titan.graphdb.relations.StandardRelation;
import com.thinkaurelius.titan.graphdb.schema.SchemaContainer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.CompositeIndexType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.MixedIndexType;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.tinkerpop.blueprints.Direction;

/**
 * Given (1) an InputFormat that iterates a Titan edgestore and converts each
 * row into a FaunusVertex and (2) the name of an already-defined index in
 * either the REGISTERED or ENABLED state, consider each FaunusVertex and
 * rebuild the named index accordingly. The index is written through a
 * TitanGraph instance. The KEYOUT and VALUEOUT type parameters are NullWritable
 * because this Mapper produces no conventional SequenceFile output. There's
 * nothing to combine or reduce since the writes go through TitanGraph and its
 * non-Hadoop backend interface.
 */
public class TitanIndexRepairMapper extends Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable> {

    private static final Logger log =
            LoggerFactory.getLogger(TitanIndexRepairMapper.class);

    private StandardTitanGraph graph;
    private ManagementSystem mgmt;
    private String indexName;
    private String indexType;
    private TitanIndex index;
    private RelationType indexRelationType;

    public enum Counters {
        SUCCESSFUL_TRANSACTIONS,
        FAILED_TRANSACTIONS,
        SUCCESSFUL_GRAPH_SHUTDOWNS,
        FAILED_GRAPH_SHUTDOWNS,
    }

    @Override
    public void setup(
            final Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>.Context context) throws IOException {
        Configuration hadoopConf = DEFAULT_COMPAT.getContextConfiguration(context);
        ModifiableHadoopConfiguration faunusConf = ModifiableHadoopConfiguration.of(hadoopConf);
        BasicConfiguration titanConf = faunusConf.getOutputConf();
        indexName = faunusConf.get(TitanHadoopConfiguration.INDEX_NAME);
        indexType = faunusConf.get(TitanHadoopConfiguration.INDEX_TYPE);

        try {
            Preconditions.checkNotNull(indexName, "Need to provide at least an index name for re-index job");
            log.info("Read index information: name={} type={}", indexName, indexType);
            graph = (StandardTitanGraph)TitanFactory.open(titanConf);
            SchemaContainer schema = new SchemaContainer(graph);
            FaunusSchemaManager typeManager = FaunusSchemaManager.getTypeManager(titanConf);
            typeManager.setSchemaProvider(schema);
            log.info("Opened graph {}", graph);
            mgmt = (ManagementSystem) graph.getManagementSystem();
            validateIndexStatus();
        } catch (final Exception e) {
            if (null != mgmt && mgmt.isOpen())
                mgmt.rollback();
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
            throw new IOException(e.getMessage(), e);
        }
    }

    @Override
    public void cleanup(final Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>.Context context) {
        try {
            if (null != mgmt && mgmt.isOpen())
                mgmt.commit();
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.SUCCESSFUL_TRANSACTIONS, 1L);
        } catch (RuntimeException e) {
            log.error("Transaction commit threw runtime exception:", e);
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
            throw e;
        }

        try {
            if (null != graph && graph.isOpen())
                graph.shutdown();
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.SUCCESSFUL_GRAPH_SHUTDOWNS, 1L);
        } catch (RuntimeException e) {
            log.error("Graph shutdown threw runtime exception:", e);
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_GRAPH_SHUTDOWNS, 1L);
            throw e;
        }
    }

    private StandardRelation getTitanRelation(StandardFaunusRelation faunusRelation, StandardTitanTx tx) {
        StandardRelation titanRelation;
        if (faunusRelation.isEdge()) {
            StandardFaunusEdge faunusEdge = (StandardFaunusEdge)faunusRelation;
            InternalVertex start = tx.getInternalVertex(faunusEdge.getVertexId(Direction.OUT)),
                    end = tx.getInternalVertex(faunusEdge.getVertexId(Direction.IN));
            if (start==null || end==null) return null;
            titanRelation = new StandardEdge(faunusRelation.getLongId(),tx.getOrCreateEdgeLabel(indexType),start,end, ElementLifeCycle.Loaded);
        } else {
            assert faunusRelation.isProperty();
            StandardFaunusProperty faunusProperty = (StandardFaunusProperty)faunusRelation;
            InternalVertex v = tx.getInternalVertex(faunusProperty.getVertex().getLongId());
            if (v==null) return null;
            titanRelation = new StandardProperty(faunusProperty.getLongId(),tx.getOrCreatePropertyKey(indexType),v,faunusProperty.getValue(), ElementLifeCycle.Loaded);
        }
        //Add properties
        for (TitanRelation rel : faunusRelation.query().relations()) {
            Object value;
            if (rel.isProperty()) value = ((FaunusProperty)rel).getValue();
            else value = tx.getInternalVertex(((FaunusEdge) rel).getVertexId(Direction.IN));
            if (value!=null) titanRelation.setProperty(rel.getType().getName(),value);
        }
        return titanRelation;
    }

    @Override
    public void map(
            final NullWritable key, // ignored
            final FaunusVertex faunusVertex,
            final Mapper<NullWritable, FaunusVertex, NullWritable, NullWritable>.Context context) throws IOException {
        try {
            StandardTitanTx tx = mgmt.getWrappedTx();
            BackendTransaction mutator = tx.getTxHandle();
            if (index instanceof RelationTypeIndex) {
                RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper)index;
                InternalRelationType wrappedType = wrapper.getWrappedType();
                EdgeSerializer edgeSerializer = ((StandardTitanGraph)graph).getEdgeSerializer();
                List<Entry> additions = new ArrayList<Entry>();

                for (TitanRelation faunusRelation : faunusVertex.query().relations()) {
                    if (!faunusRelation.getType().getName().equals(indexType) ||
                            faunusRelation.getDirection(faunusVertex)!=Direction.OUT) continue; //Isolate relevant relations and only outgoing ones
                    StandardRelation titanRelation = getTitanRelation((StandardFaunusRelation)faunusRelation,tx);
                    for (int pos = 0; pos < titanRelation.getArity(); pos++) {
                        if (!wrappedType.isUnidirected(Direction.BOTH) && !wrappedType.isUnidirected(EdgeDirection.fromPosition(pos)))
                            continue; //Directionality is not covered
                        Entry entry = edgeSerializer.writeRelation(titanRelation, wrappedType, pos, tx);
                        additions.add(entry);
                    }
                }
                StaticBuffer vertexKey = graph.getIDManager().getKey(faunusVertex.getLongId());
                mutator.mutateEdges(vertexKey, additions, KCVSCache.NO_DELETIONS);
            } else if (index instanceof TitanGraphIndex) {
                IndexType indexType = mgmt.getSchemaVertex(index).asIndexType();
                assert indexType!=null;
                IndexSerializer indexSerializer = graph.getIndexSerializer();
                //Gather elements to index
                List<FaunusElement> elements;
                switch (indexType.getElement()) {
                    case VERTEX:
                        elements = ImmutableList.of((FaunusElement)faunusVertex);
                        break;
                    case PROPERTY:
                        elements = Lists.newArrayList();
                        for (TitanProperty p : faunusVertex.query().properties()) {
                            elements.add((StandardFaunusProperty)p);
                        }
                        break;
                    case EDGE:
                        elements = Lists.newArrayList();
                        for (TitanEdge e : faunusVertex.query().titanEdges()) {
                            elements.add((StandardFaunusEdge)e);
                        }
                        break;
                    default: throw new AssertionError("Unexpected category: " + indexType.getElement());
                }
                if (indexType.isCompositeIndex()) {
                    for (FaunusElement element : elements) {
                        Set<IndexSerializer.IndexUpdate<StaticBuffer,Entry>> updates =
                            indexSerializer.reindexElement(element, (CompositeIndexType) indexType);
                        for (IndexSerializer.IndexUpdate<StaticBuffer,Entry> update : updates) {
                            log.debug("Mutating index {}: {}", indexType, update.getEntry());
                            mutator.mutateIndex(update.getKey(), Lists.newArrayList(update.getEntry()), KCVSCache.NO_DELETIONS);
                        }
                    }
                } else {
                    assert indexType.isMixedIndex();
                    Map<String,Map<String,List<IndexEntry>>> documentsPerStore = Maps.newHashMap();
                    for (FaunusElement element : elements) {
                        indexSerializer.reindexElement(element, (MixedIndexType) indexType, documentsPerStore);
                    }
                    mutator.getIndexTransaction(indexType.getBackingIndexName()).restore(documentsPerStore);
                }

            } else throw new UnsupportedOperationException("Unsupported index found: "+index);

//            log.info("Committing mutator {} in mapper {}", mutator, this);
//            mutator.commit();
        } catch (final Exception e) {
            mgmt.rollback();
            DEFAULT_COMPAT.incrementContextCounter(context, Counters.FAILED_TRANSACTIONS, 1L);
            throw new IOException(e.getMessage(), e);
        }
    }

    /**
     * Check that our target index is in either the ENABLED or REGISTERED state.
     */
    private void validateIndexStatus() {
        if (indexType==null || StringUtils.isBlank(indexType)) {
            index = mgmt.getGraphIndex(indexName);
        } else {
            indexRelationType = mgmt.getRelationType(indexType);
            Preconditions.checkArgument(indexRelationType!=null,"Could not find relation type: %s",indexType);
            index = mgmt.getRelationIndex(indexRelationType,indexName);
        }
        Preconditions.checkArgument(index!=null,"Could not find index: %s",indexName);
        log.info("Found index {}", indexName);
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
            Map<String, SchemaStatus> invalidKeyStatuses = Maps.newHashMap();
            for (PropertyKey key : gindex.getFieldKeys()) {
                SchemaStatus status = gindex.getIndexStatus(key);
                if (status!=SchemaStatus.DISABLED && !acceptableStatuses.contains(status)) {
                    isValidIndex=false;
                    invalidKeyStatuses.put(key.getName(), status);
                    log.warn("Index {} has key {} in an invalid status {}",index,key,status);
                }
            }
            invalidIndexHint = String.format(
                    "The following index keys have invalid status: %s (status must be one of %s)",
                    Joiner.on(",").withKeyValueSeparator(" has status ").join(invalidKeyStatuses), acceptableStatuses);
        }
        Preconditions.checkArgument(isValidIndex, "The index %s is in an invalid state and cannot be indexed. %s", indexName, invalidIndexHint);
        // TODO consider retrieving the current Job object and calling killJob() if !isValidIndex -- would be more efficient than throwing an exception on the first pair processed by each mapper
        log.debug("Index {} is valid for re-indexing");
    }
}
