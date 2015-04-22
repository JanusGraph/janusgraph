package com.thinkaurelius.titan.diskstorage.keycolumnvalue.ttl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.thinkaurelius.titan.diskstorage.BackendException;
import com.thinkaurelius.titan.diskstorage.Entry;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.MetaAnnotatable;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.StoreMetaData;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVSManagerProxy;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStore;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StandardStoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreFeatures;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Collection;
import java.util.Map;

import static com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration.STORE_TTL_SECONDS;

/**
 * Turns a store with fine-grained cell-level TTL support into a store
 * with coarse-grained store-level (columnfamily-level) TTL support.
 * Useful when running a KCVSLog atop Cassandra.  Cassandra has
 * cell-level TTL support, but KCVSLog just wants to write all of its
 * data with a fixed, CF-wide TTL.  This class stores a fixed TTL set
 * during construction and applies it to every entry written through
 * subsequent mutate/mutateMany calls.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TTLKCVSManager extends KCVSManagerProxy {

    private final StoreFeatures features;
    private final Map<String, Integer> ttlEnabledStores = Maps.newConcurrentMap();

    public TTLKCVSManager(KeyColumnValueStoreManager manager) {
        super(manager);
        Preconditions.checkArgument(supportsStoreTTL(manager),
                "Wrapped store must support cell or store level TTL: %s", manager);
        Preconditions.checkArgument(manager.getFeatures().hasCellTTL());
        this.features = new StandardStoreFeatures.Builder(manager.getFeatures()).storeTTL(true).build();
    }

    public static boolean supportsStoreTTL(KeyColumnValueStoreManager manager) {
        return supportsStoreTTL(manager.getFeatures());
    }

    public static boolean supportsStoreTTL(StoreFeatures features) {
        return features.hasCellTTL() || features.hasStoreTTL();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws BackendException {
        return openDatabase(name, ImmutableMap.of());
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, Map<StoreMetaData, Object> metaData) throws BackendException {
        KeyColumnValueStore store = manager.openDatabase(name);
        int storeTTL = -1;
        if (metaData.containsKey(StoreMetaData.TTL)) {
            storeTTL = (Integer) metaData.get(StoreMetaData.TTL);
        }
        Preconditions.checkArgument(storeTTL>0,"TTL must be positive: %s", storeTTL);
        ttlEnabledStores.put(name, storeTTL);
        return new TTLKCVS(store, storeTTL);
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (!manager.getFeatures().hasStoreTTL()) {
            assert manager.getFeatures().hasCellTTL();
            for (Map.Entry<String,Map<StaticBuffer, KCVMutation>> sentry : mutations.entrySet()) {
                Integer ttl = ttlEnabledStores.get(sentry.getKey());
                if (null != ttl && 0 < ttl) {
                    for (KCVMutation mut : sentry.getValue().values()) {
                        if (mut.hasAdditions()) applyTTL(mut.getAdditions(), ttl);
                    }
                }
            }
        }
        manager.mutateMany(mutations,txh);
    }

    public static void applyTTL(Collection<Entry> additions, int ttl) {
        for (Entry entry : additions) {
            assert entry instanceof MetaAnnotatable;
            ((MetaAnnotatable)entry).setMetaData(EntryMetaData.TTL,ttl);
        }
    }


}
