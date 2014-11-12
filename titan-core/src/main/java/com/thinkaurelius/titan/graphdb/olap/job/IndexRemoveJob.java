package com.thinkaurelius.titan.graphdb.olap.job;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanException;
import com.thinkaurelius.titan.core.TitanGraph;
import com.thinkaurelius.titan.core.schema.RelationTypeIndex;
import com.thinkaurelius.titan.core.schema.SchemaStatus;
import com.thinkaurelius.titan.core.schema.TitanGraphIndex;
import com.thinkaurelius.titan.diskstorage.BackendTransaction;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache.KCVSCache;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanJob;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.scan.ScanMetrics;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.IndexSerializer;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.management.ManagementSystem;
import com.thinkaurelius.titan.graphdb.database.management.RelationTypeIndexWrapper;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.olap.QueryContainer;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.CompositeIndexType;
import com.thinkaurelius.titan.graphdb.types.IndexType;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.tinkerpop.gremlin.structure.Direction;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexRemoveJob extends IndexUpdateJob implements ScanJob {

    private long graphIndexId;

    public IndexRemoveJob(final String indexName, final String indexType) {
        super(indexName,indexType);
    }

    @Override
    public void teardown(ScanMetrics metrics) {
        super.teardown(metrics);
    }

    @Override
    public void setup(TitanGraph graph, Configuration config, ScanMetrics metrics) {
        super.setup(graph, config, metrics);
    }

    @Override
    protected void validateIndexStatus() {
        if (index instanceof RelationTypeIndex) {
            //Nothing specific to be done
        } else if (index instanceof TitanGraphIndex) {
            TitanGraphIndex gindex = (TitanGraphIndex)index;
            if (gindex.isMixedIndex())
                throw new UnsupportedOperationException("Cannot remove mixed indexes through Titan. This can " +
                        "only be accomplished in the indexing system directly.");
            CompositeIndexType indexType = (CompositeIndexType)mgmt.getSchemaVertex(index).asIndexType();
            graphIndexId = indexType.getID();
        } else throw new UnsupportedOperationException("Unsupported index found: "+index);

        //Must be a relation type index or a composite graph index
        TitanSchemaVertex schemaVertex = mgmt.getSchemaVertex(index);
        SchemaStatus actualStatus = schemaVertex.getStatus();
        Preconditions.checkArgument(actualStatus==SchemaStatus.DISABLED,"The index [%s] must be disabled before it can be removed",indexName);
    }

    @Override
    public void process(StaticBuffer key, Map<SliceQuery, EntryList> entries, ScanMetrics metrics) {
        //The queries are already tailored enough => everything should be removed
        try {
            StandardTitanTx tx = mgmt.getWrappedTx();
            BackendTransaction mutator = tx.getTxHandle();
            final List<Entry> deletions;
            if (entries.size()==1) deletions = Iterables.getOnlyElement(entries.values());
            else {
                int size = StreamFactory.stream(entries.values()).map( e -> e.size()).reduce(0, (x,y) -> x+y);
                deletions = new ArrayList<>(size);
                entries.values().forEach(e -> deletions.addAll(e));
            }

            if (isRelationTypeIndex()) {
                mutator.mutateEdges(key, KCVSCache.NO_ADDITIONS, deletions);
            } else {
                mutator.mutateIndex(key, KCVSCache.NO_ADDITIONS, deletions);
            }
        } catch (final Exception e) {
            mgmt.rollback();
            metrics.incrementCustom(FAILED_TX);
            throw new TitanException(e.getMessage(), e);
        }
    }

    @Override
    public List<SliceQuery> getQueries() {
        if (isGlobalGraphIndex()) {
            //Everything
            return ImmutableList.of(new SliceQuery(BufferUtil.zeroBuffer(128), BufferUtil.oneBuffer(128)));
        } else {
            RelationTypeIndexWrapper wrapper = (RelationTypeIndexWrapper)index;
            InternalRelationType wrappedType = wrapper.getWrappedType();
            Direction direction=null;
            for (Direction dir : Direction.values()) if (wrappedType.isUnidirected(dir)) direction=dir;
            assert direction!=null;

            StandardTitanTx tx = (StandardTitanTx)graph.newTransaction();
            QueryContainer qc = new QueryContainer(tx);
            qc.addQuery().type(wrappedType).direction(direction).relations();
            return qc.getSliceQueries();
        }
    }

    @Override
    public Predicate<StaticBuffer> getKeyFilter() {
        if (isGlobalGraphIndex()) {
            final IndexSerializer indexSerializer = graph.getIndexSerializer();
            assert graphIndexId>0;
            return (k -> indexSerializer.getIndexIdFromKey(k)==graphIndexId);
        } else return (k -> true);
    }
}
