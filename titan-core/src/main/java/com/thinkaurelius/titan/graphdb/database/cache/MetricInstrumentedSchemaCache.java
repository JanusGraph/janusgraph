package com.thinkaurelius.titan.graphdb.database.cache;

import com.thinkaurelius.titan.diskstorage.EntryList;
import com.thinkaurelius.titan.diskstorage.util.CacheMetricsAction;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.SystemType;
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
            public Long retrieveTypeByName(String typeName, StandardTitanTx tx) {
                incAction(METRICS_TYPENAME,CacheMetricsAction.MISS,tx);
                return retriever.retrieveTypeByName(typeName,tx);
            }

            @Override
            public EntryList retrieveTypeRelations(long schemaId, SystemType type, Direction dir, StandardTitanTx tx) {
                incAction(METRICS_RELATIONS,CacheMetricsAction.MISS,tx);
                return retriever.retrieveTypeRelations(schemaId,type,dir,tx);
            }
        });
    }

    private void incAction(String type, CacheMetricsAction action, StandardTitanTx tx) {
        if (tx.getConfiguration().getMetricsPrefix()!=null) {
            MetricManager.INSTANCE.getCounter(tx.getConfiguration().getMetricsPrefix(), METRICS_NAME, type, action.getName()).inc();
        }
    }

    @Override
    public Long getTypeId(String typeName, StandardTitanTx tx) {
        incAction(METRICS_TYPENAME,CacheMetricsAction.RETRIEVAL,tx);
        return cache.getTypeId(typeName,tx);
    }

    @Override
    public EntryList getTypeRelations(long schemaId, SystemType type, Direction dir, StandardTitanTx tx) {
        incAction(METRICS_RELATIONS,CacheMetricsAction.RETRIEVAL,tx);
        return cache.getTypeRelations(schemaId, type, dir, tx);
    }

    @Override
    public void expireTypeName(String name) {
        cache.expireTypeName(name);
    }

    @Override
    public void expireTypeRelations(long schemaId) {
        cache.expireTypeRelations(schemaId);
    }

}
