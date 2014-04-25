package com.thinkaurelius.titan.diskstorage.keycolumnvalue.cache;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.time.Duration;
import com.thinkaurelius.titan.diskstorage.*;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KCVMutation;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.StoreTransaction;
import com.thinkaurelius.titan.diskstorage.util.BackendOperation;
import com.thinkaurelius.titan.diskstorage.util.BufferUtil;
import com.thinkaurelius.titan.graphdb.database.idhandling.VariableLong;
import com.thinkaurelius.titan.graphdb.database.serialize.DataOutput;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.*;
import java.util.concurrent.Callable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class CacheTransaction implements StoreTransaction, LoggableTransaction {

    private static final Logger log =
            LoggerFactory.getLogger(CacheTransaction.class);

    private final StoreTransaction tx;
    private final KeyColumnValueStoreManager manager;
    private final boolean continuousPersistence;
    private final int persistChunkSize;
    private final int mutationAttempts;
    private final Duration attemptWaitTime;

    private int numMutations;
    private final Map<KCVSCache, Map<StaticBuffer, KCVMutation>> mutations;

    public CacheTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager,
                             int persistChunkSize, int attempts, Duration waitTime, boolean continuousPersistence) {
        this(tx, manager, persistChunkSize, attempts, waitTime, continuousPersistence, 2);
    }

    public CacheTransaction(StoreTransaction tx, KeyColumnValueStoreManager manager, int persistChunkSize,
                            int attempts, Duration waitTime, boolean continuousPersistence, int expectedNumStores) {
        Preconditions.checkArgument(tx != null && manager != null && persistChunkSize > 0);
        this.tx = tx;
        this.manager = manager;
        this.continuousPersistence=continuousPersistence;
        this.numMutations = 0;
        this.persistChunkSize = persistChunkSize;
        this.mutationAttempts = attempts;
        this.attemptWaitTime = waitTime;
        this.mutations = new HashMap<KCVSCache, Map<StaticBuffer, KCVMutation>>(expectedNumStores);
    }

    public StoreTransaction getWrappedTransactionHandle() {
        return tx;
    }

    void mutate(KCVSCache store, StaticBuffer key, List<Entry> additions, List<StaticBuffer> deletions) throws StorageException {
        Preconditions.checkNotNull(store);
        if (additions.isEmpty() && deletions.isEmpty()) return;

        KCVMutation m = new KCVMutation(additions, deletions);
        Map<StaticBuffer, KCVMutation> storeMutation = mutations.get(store);
        if (storeMutation == null) {
            storeMutation = new HashMap<StaticBuffer, KCVMutation>();
            mutations.put(store, storeMutation);
        }
        KCVMutation existingM = storeMutation.get(key);
        if (existingM != null) {
            existingM.merge(m);
        } else {
            storeMutation.put(key, m);
        }

        numMutations += m.getTotalMutations();

        if (continuousPersistence && numMutations >= persistChunkSize) {
            flushInternal();
        }
    }

    private int persist(final Map<String, Map<StaticBuffer, KCVMutation>> subMutations) {
        BackendOperation.execute(new Callable<Boolean>() {
            @Override
            public Boolean call() throws Exception {
                manager.mutateMany(subMutations, tx);
                return true;
            }

            @Override
            public String toString() {
                return "CacheMutation";
            }
        }, mutationAttempts, attemptWaitTime);
        subMutations.clear();
        return 0;
    }

    private int mutationSize(Map<StaticBuffer, KCVMutation> mutation) {
        int size = 0;
        for (KCVMutation mut : mutation.values()) size+=mut.getTotalMutations();
        return size;
    }

    private static final Function<Entry,StaticBuffer> CONSOLIDATION_FCT = new Function<Entry, StaticBuffer>() {
        @Nullable
        @Override
        public StaticBuffer apply(@Nullable Entry entry) {
            return entry.getColumn();
        }
    };

    private void flushInternal() throws StorageException {
        if (numMutations > 0) {
            //Consolidate mutations
            for (Map<StaticBuffer, KCVMutation> store : mutations.values()) {
                for (KCVMutation mut : store.values()) mut.consolidate(CONSOLIDATION_FCT);
            }

            //Chuck up mutations
            final Map<String, Map<StaticBuffer, KCVMutation>> subMutations = new HashMap<String, Map<StaticBuffer, KCVMutation>>(mutations.size());
            int numSubMutations = 0;
            for (Map.Entry<KCVSCache,Map<StaticBuffer, KCVMutation>> storeMuts : mutations.entrySet()) {
                int mutSize = mutationSize(storeMuts.getValue());
                if (mutSize==0) continue;
                if ((numSubMutations+mutSize)*3/2<= persistChunkSize) {
                    subMutations.put(storeMuts.getKey().getName(),storeMuts.getValue());
                    numSubMutations+=mutSize;
                    if (numSubMutations>= persistChunkSize) numSubMutations = persist(subMutations);
                } else { //Split it up
                    Map<StaticBuffer, KCVMutation> sub = new HashMap<StaticBuffer, KCVMutation>();
                    subMutations.put(storeMuts.getKey().getName(),sub);
                    for (Map.Entry<StaticBuffer,KCVMutation> muts : storeMuts.getValue().entrySet()) {
                        sub.put(muts.getKey(),muts.getValue());
                        numSubMutations+=muts.getValue().getTotalMutations();
                        if (numSubMutations>= persistChunkSize) {
                            numSubMutations = persist(subMutations);
                            sub.clear();
                            subMutations.put(storeMuts.getKey().getName(),sub);
                        }
                    }
                }
            }
            if (numSubMutations>0) persist(subMutations);


            for (Map.Entry<KCVSCache,Map<StaticBuffer, KCVMutation>> storeMuts : mutations.entrySet()) {
                KCVSCache cache = storeMuts.getKey();
                for (Map.Entry<StaticBuffer,KCVMutation> muts : storeMuts.getValue().entrySet()) {
                    if (cache.hasValidateKeysOnly()) {
                        cache.invalidate(muts.getKey(), Collections.EMPTY_LIST);
                    } else {
                        KCVMutation m = muts.getValue();
                        List<CachableStaticBuffer> entries = new ArrayList<CachableStaticBuffer>(m.getTotalMutations());
                        for (Entry e : m.getAdditions()) {
                            assert e instanceof CachableStaticBuffer;
                            entries.add((CachableStaticBuffer)e);
                        }
                        for (StaticBuffer e : m.getDeletions()) {
                            assert e instanceof CachableStaticBuffer;
                            entries.add((CachableStaticBuffer)e);
                        }
                        cache.invalidate(muts.getKey(),entries);
                    }
                }
            }
            clear();
        }
    }

    private void clear() {
        for (Map.Entry<KCVSCache, Map<StaticBuffer, KCVMutation>> entry : mutations.entrySet()) {
            entry.getValue().clear();
        }
        numMutations = 0;
    }

    @Override
    public void logMutations(DataOutput out) {
        Preconditions.checkArgument(!continuousPersistence,"Cannot log entire mutation set when continuous persistence is enabled");
        VariableLong.writePositive(out,mutations.size());
        for (Map.Entry<KCVSCache,Map<StaticBuffer, KCVMutation>> storeMuts : mutations.entrySet()) {
            out.writeObjectNotNull(storeMuts.getKey().getName());
            VariableLong.writePositive(out,storeMuts.getValue().size());
            for (Map.Entry<StaticBuffer,KCVMutation> muts : storeMuts.getValue().entrySet()) {
                BufferUtil.writeBuffer(out,muts.getKey());
                KCVMutation mut = muts.getValue();
                List<Entry> adds = mut.getAdditions();
                VariableLong.writePositive(out,adds.size());
                for (Entry add : adds) BufferUtil.writeEntry(out,add);
                List<StaticBuffer> dels = mut.getDeletions();
                VariableLong.writePositive(out,dels.size());
                for (StaticBuffer del: dels) BufferUtil.writeBuffer(out,del);
            }
        }
    }

    @Override
    public void flush() throws StorageException {
        flushInternal();
        tx.flush();
    }

    @Override
    public void commit() throws StorageException {
        flushInternal();
        tx.commit();
    }

    @Override
    public void rollback() throws StorageException {
        clear();
        tx.rollback();
    }

    @Override
    public TransactionHandleConfig getConfiguration() {
        return tx.getConfiguration();
    }

}
