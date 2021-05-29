// Copyright 2019 JanusGraph Authors
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

package org.janusgraph.diskstorage.inmemory;

import com.google.common.base.Preconditions;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.StoreMetaData;
import org.janusgraph.diskstorage.common.AbstractStoreTransaction;
import org.janusgraph.diskstorage.configuration.Configuration;
import org.janusgraph.diskstorage.keycolumnvalue.KCVMutation;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStore;
import org.janusgraph.diskstorage.keycolumnvalue.KeyColumnValueStoreManager;
import org.janusgraph.diskstorage.keycolumnvalue.KeyRange;
import org.janusgraph.diskstorage.keycolumnvalue.StandardStoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreFeatures;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;
import org.janusgraph.graphdb.configuration.GraphDatabaseConfiguration;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.stream.Collectors;

/**
 * In-memory backend storage engine.
 *
 */

public class InMemoryStoreManager implements KeyColumnValueStoreManager {

    private ConcurrentHashMap<String, InMemoryKeyColumnValueStore> stores;

    private final StoreFeatures features;

    public InMemoryStoreManager() {
        this(Configuration.EMPTY);
    }

    public InMemoryStoreManager(final Configuration configuration) {

        stores = new ConcurrentHashMap<>();

        features = new StandardStoreFeatures.Builder()
            .orderedScan(true)
            .unorderedScan(true)
            .keyOrdered(true)
            .persists(false)
            .optimisticLocking(true)
            .keyConsistent(GraphDatabaseConfiguration.buildGraphConfiguration())
            .build();
    }

    @Override
    public StoreTransaction beginTransaction(final BaseTransactionConfig config) throws BackendException {
        return new InMemoryTransaction(config);
    }

    @Override
    public void close() throws BackendException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.close();
        }
        stores.clear();
    }

    @Override
    public void clearStorage() throws BackendException {
        for (InMemoryKeyColumnValueStore store : stores.values()) {
            store.clear();
        }
        stores.clear();
    }

    @Override
    public boolean exists() throws BackendException {
        return !stores.isEmpty();
    }

    @Override
    public StoreFeatures getFeatures() {
        return features;
    }

    @Override
    public KeyColumnValueStore openDatabase(final String name, StoreMetaData.Container metaData) throws BackendException {
        if (!stores.containsKey(name)) {
            stores.putIfAbsent(name, new InMemoryKeyColumnValueStore(name));
        }
        KeyColumnValueStore store = stores.get(name);
        Preconditions.checkNotNull(store);
        return store;
    }

    @Override
    public void mutateMany(Map<String, Map<StaticBuffer, KCVMutation>> mutations, StoreTransaction txh) throws BackendException {
        for (Map.Entry<String, Map<StaticBuffer, KCVMutation>> storeMut : mutations.entrySet()) {
            KeyColumnValueStore store = stores.get(storeMut.getKey());
            Preconditions.checkNotNull(store);
            for (Map.Entry<StaticBuffer, KCVMutation> keyMut : storeMut.getValue().entrySet()) {
                store.mutate(keyMut.getKey(), keyMut.getValue().getAdditions(), keyMut.getValue().getDeletions(), txh);
            }
        }
    }

    @Override
    public List<KeyRange> getLocalKeyPartition() throws BackendException {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return toString();
    }

    public void makeSnapshot(File targetSnapshotDirectory, ForkJoinPool parallelOperationsExecutor) throws IOException {
        Files.createDirectory(Paths.get(targetSnapshotDirectory.getAbsolutePath()));

        stores.entrySet().stream().map(e -> parallelOperationsExecutor.submit(() ->
        {
            try {
                dumpStore(e.getKey(), e.getValue(), targetSnapshotDirectory.getAbsolutePath(), parallelOperationsExecutor);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        })).collect(Collectors.toList()).stream() //force it to submit all tasks
            .map(ForkJoinTask::join).collect(Collectors.toList());
    }

    private void dumpStore(String storeName, InMemoryKeyColumnValueStore store, String rootPath, ForkJoinPool parallelOperationsExecutor) throws IOException {
        Path filePath = Paths.get(rootPath, storeName);

        Files.createDirectory(filePath);

        store.dumpTo(filePath, parallelOperationsExecutor);
    }

    public void restoreFromSnapshot(File sourceSnapshotDirectory, boolean rollbackIfFailed, ForkJoinPool parallelOperationsExecutor) throws IOException, BackendException {
        final Path root = Paths.get(sourceSnapshotDirectory.getAbsolutePath());
        if (!rollbackIfFailed) {
            //NOTE: if rollbackIfFailed is false, we clear current contents of stores before loading, thus freeing up memory to load new data,
            //but we lose the ability to go back to old data if load failed
            clearStorage();
        }

        ConcurrentHashMap<String, InMemoryKeyColumnValueStore> newStores = rollbackIfFailed ? new ConcurrentHashMap<>(stores.size()) : stores;

        //this assumes that any subdirectory is a dumped store, ignores non-directories such as serializer mappings file etc
        Files.list(root).filter(path -> path.toFile().isDirectory()).map(storePath -> parallelOperationsExecutor.submit(() ->
        {
            try {
                newStores.put(storePath.getFileName().toString(), InMemoryKeyColumnValueStore.readFrom(storePath, storePath.getFileName().toString(), parallelOperationsExecutor));
            } catch (IOException ex) {
                throw new RuntimeException(ex);
            }
        })).collect(Collectors.toList()).stream() //force it to submit all tasks
            .map(ForkJoinTask::join).collect(Collectors.toList());

        if (rollbackIfFailed) {
            //NOTE: if rollbackIfFailed is true, we clear current contents of stores only AFTER successful loading,
            // thus having the option to get back to old data if load failed, but we then require 2x memory to hold both old and new data
            // so the risk of OOM increases
            clearStorage();
            stores = newStores;
        }
    }

    private static class InMemoryTransaction extends AbstractStoreTransaction {

        public InMemoryTransaction(final BaseTransactionConfig config) {
            super(config);
        }
    }
}
