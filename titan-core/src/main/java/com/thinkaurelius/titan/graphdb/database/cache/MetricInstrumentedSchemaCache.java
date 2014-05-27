package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.util.CacheMetricsAction;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.BaseRelationType;
import com.thinkaurelius.titan.graphdb.types.system.SystemRelationType;
import com.thinkaurelius.titan.util.stats.MetricManager;
import com.tinkerpop.blueprints.Direction;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class MetricInstrumentedSchemaCache implements SchemaCache {

    public static final String METRICS_NAME = "schemacache";

    public static final String METRICS_TYPENAME = "name";
    public static final String METRICS_RELATIONS = "relations";

    private final SchemaCache cache;

    public MetricInstrumentedSchemaCache(final StoreRetrieval retriever) {
        cache = new StandardSchemaCache(new StoreRetrieval() {
            @Override
            public Long retrieveSchemaByName(String typeName, StandardTitanTx tx) {
                incAction(METRICS_TYPENAME,CacheMetricsAction.MISS,tx);
                return retriever.retrieveSchemaByName(typeName, tx);
            }

            @Override
            public EntryList retrieveSchemaRelations(long schemaId, BaseRelationType type, Direction dir, StandardTitanTx tx) {
                incAction(METRICS_RELATIONS,CacheMetricsAction.MISS,tx);
                return retriever.retrieveSchemaRelations(schemaId, type, dir, tx);
            }
        });
    }

    private void incAction(String type, CacheMetricsAction action, StandardTitanTx tx) {
        if (tx.getConfiguration().getGroupName()!=null) {
            MetricManager.INSTANCE.getCounter(tx.getConfiguration().getGroupName(), METRICS_NAME, type, action.getName()).inc();
        }
    }

    @Override
    public Long getSchemaId(String schemaName, StandardTitanTx tx) {
        incAction(METRICS_TYPENAME,CacheMetricsAction.RETRIEVAL,tx);
        return cache.getSchemaId(schemaName, tx);
    }

    @Override
    public EntryList getSchemaRelations(long schemaId, BaseRelationType type, Direction dir, StandardTitanTx tx) {
        incAction(METRICS_RELATIONS,CacheMetricsAction.RETRIEVAL,tx);
        return cache.getSchemaRelations(schemaId, type, dir, tx);
    }

    @Override
    public void expireSchemaName(String name) {
        cache.expireSchemaName(name);
    }

    @Override
    public void expireSchemaRelations(long schemaId) {
        cache.expireSchemaRelations(schemaId);
    }

}
