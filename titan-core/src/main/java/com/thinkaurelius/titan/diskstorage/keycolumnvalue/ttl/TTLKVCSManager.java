package com.thinkaurelius.titan.diskstorage.keycolumnvalue.ttl;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.configuration.Configuration;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.*;
import com.thinkaurelius.titan.graphdb.configuration.GraphDatabaseConfiguration;
import com.thinkaurelius.titan.util.encoding.ConversionHelper;
import org.apache.commons.lang.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TTLKVCSManager extends KCVSManagerProxy implements CustomizeStoreKCVSManager {

    private final StoreFeatures features;
    private final Map<String,Integer> ttlEnabledStores;
    private final int defaultTTL;

    public TTLKVCSManager(KeyColumnValueStoreManager manager, int defaultTTL) {
        super(manager);
        Preconditions.checkArgument(supportsStoreTTL(manager),
                "Wrapped store must support cell or store level TTL: %s", manager);
        Preconditions.checkArgument(defaultTTL>0,"Default TTL must b > 0: %s",defaultTTL);
        Preconditions.checkArgument(!manager.getFeatures().hasStoreTTL() || (manager instanceof CustomizeStoreKCVSManager));
        this.defaultTTL = defaultTTL;
        this.features = new StandardStoreFeatures.Builder(manager.getFeatures()).storeTTL(true).build();
        this.ttlEnabledStores = Maps.newHashMap();
    }

    public static boolean supportsStoreTTL(KeyColumnValueStoreManager manager) {
        return supportsStoreTTL(manager.getFeatures());
    }

    public static boolean supportsStoreTTL(StoreFeatures features) {
        return features.hasCellTTL() || features.hasStoreTTL();
    }

    public synchronized void setTTL(String storeName, int ttl) {
        Preconditions.checkArgument(StringUtils.isNotBlank(storeName));
        Preconditions.checkArgument(ttl>0);
        Preconditions.checkArgument(!ttlEnabledStores.containsKey(storeName),"A TTL has already been set for store: %s",storeName);
        ttlEnabledStores.put(storeName,ttl);
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(String name) throws BackendException {
        return openDatabase(name,getTTL(name));
    }

    @Override
    public KeyColumnValueStore openDatabase(String name, int ttlInSeconds) throws BackendException {
        if (manager.getFeatures().hasStoreTTL()) {
            return ((CustomizeStoreKCVSManager)manager).openDatabase(name,ttlInSeconds);
        } else {
            assert manager.getFeatures().hasCellTTL();
            KeyColumnValueStore store = manager.openDatabase(name);
            return new TTLKCVS(store,ttlInSeconds);
        }
    }

    private final int getTTL(String storeName) {
        int ttl = defaultTTL;
        if (ttlEnabledStores.containsKey(storeName)) ttl = ttlEnabledStores.get(storeName);
        return ttl;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        if (!manager.getFeatures().hasStoreTTL()) {
            assert manager.getFeatures().hasCellTTL();
            for (Map.Entry<String,Map<StaticBuffer, KCVMutation>> sentry : mutations.entrySet()) {
                Integer ttl = ttlEnabledStores.get(sentry.getKey());
                if (ttl!=null) {
                    for (KCVMutation mut : sentry.getValue().values()) {
                        if (mut.hasAdditions()) applyTTL(mut.getAdditions(),ttl);
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
