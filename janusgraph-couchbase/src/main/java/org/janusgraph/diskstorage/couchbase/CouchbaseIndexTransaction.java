/*
 * Copyright 2023 Couchbase, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.janusgraph.diskstorage.couchbase;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.Collection;
import com.couchbase.client.java.Scope;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.manager.collection.CollectionSpec;
import com.couchbase.client.java.manager.query.BuildQueryIndexOptions;
import com.couchbase.client.java.manager.query.CreateQueryIndexOptions;
import com.couchbase.client.java.manager.query.DropQueryIndexOptions;
import com.couchbase.client.java.manager.query.QueryIndex;
import com.couchbase.client.java.manager.search.SearchIndex;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.BaseTransactionConfig;
import org.janusgraph.diskstorage.BaseTransactionConfigurable;
import org.janusgraph.diskstorage.indexing.IndexEntry;
import org.janusgraph.diskstorage.indexing.IndexMutation;
import org.janusgraph.diskstorage.indexing.KeyInformation;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class CouchbaseIndexTransaction implements BaseTransactionConfigurable {

    private final Cluster cluster;

    private final Bucket bucket;

    private final Scope scope;
    private final String indexNamespace;

    private Map<String, Map<String, List<IndexMutation>>> mutations = new HashMap<>();
    private Map<String, Map<Mapping, Map<String, KeyInformation>>> keys = new HashMap<>();
    private final String indexNamePrefix;
    private final BaseTransactionConfig config;
    private final Map<String, Map<String, List<IndexEntry>>> restore = new HashMap<>();

    public CouchbaseIndexTransaction(BaseTransactionConfig config, Cluster cluster, Bucket bucket, Scope scope, String indexNamePrefix, String indexNamespace) {
        this.config = config;
        this.cluster = cluster;
        this.bucket = bucket;
        this.scope = scope;
        this.indexNamePrefix = indexNamePrefix;
        this.indexNamespace = indexNamespace;
    }

    @Override
    public void commit() throws BackendException {
        applyKeys();
        applyMutations();
        applyRestorations();
    }

    @Override
    public void rollback() throws BackendException {
        mutations.clear();
        restore.clear();
        keys.clear();
    }

    public void register(String storeName, String key, KeyInformation information) {
        if (!this.keys.containsKey(storeName)) {
            this.keys.put(storeName, new HashMap<>());
        }
        Map<Mapping, Map<String, KeyInformation>> index = this.keys.get(storeName);
        Mapping jgMapping = Mapping.getMapping(information);

        if (!index.containsKey(jgMapping)) {
            index.put(jgMapping, new HashMap<>());
        }

        index.get(jgMapping).put(key, information);
    }

    public void mutate(Map<String, Map<String, IndexMutation>> mutations, KeyInformation.IndexRetriever information) throws BackendException {
        mutations.entrySet().forEach(storageMutations -> {
            final String storageName = storageMutations.getKey();
            if (!this.mutations.containsKey(storageName)) {
                this.mutations.put(storageName, new HashMap<>());
            }
            final Map<String, List<IndexMutation>> thisStorageMutations = this.mutations.get(storageName);
            storageMutations.getValue().entrySet().forEach(storageMutation -> {
                final String docId = storageMutation.getKey();
                if (!thisStorageMutations.containsKey(docId)) {
                    thisStorageMutations.put(docId, new LinkedList<>());
                }
                thisStorageMutations.get(docId).add(storageMutation.getValue());
            });
        });
    }

    protected void applyKeys() {
        keys.entrySet().forEach(mappings -> {
            String index = mappings.getKey();
            mappings.getValue().entrySet().forEach(indexKeys -> {
                upsertFtsIndex(index, indexKeys.getValue());
                updateIndex(index, indexKeys.getValue());
            });
        });
        keys.clear();
    }

    protected void applyMutations() {
        mutations.entrySet().parallelStream()
                .forEach(storageMutations -> {
                    final Collection storage = getStorage(storageMutations.getKey());
                    storageMutations.getValue().entrySet().parallelStream()
                            .forEach(docMutation -> {
                                final String docId = CouchbaseColumnConverter.toId(docMutation.getKey());
                                docMutation.getValue().forEach(mutation -> {
                                    JsonObject target = null;
                                    if (mutation.hasDeletions()) {
                                        if (mutation.isDeleted()) {
                                            storage.remove(docId);
                                        } else {
                                            target = storage.get(docId).contentAsObject();
                                            mutation.getDeletions().stream()
                                                    .map(ie -> ie.field)
                                                    .forEach(target::removeKey);
                                            storage.upsert(docId, target);
                                        }
                                    }
                                    if (mutation.hasAdditions()) {
                                        if (mutation.isNew()) {
                                            target = JsonObject.create();
                                            target.put("__document_key", docMutation.getKey());
                                        } else {
                                            target = storage.get(docId).contentAsObject();
                                        }

                                        for (IndexEntry addition : mutation.getAdditions()) {
                                            target.put(addition.field, IndexValueConverter.marshall(addition.value));
                                        }
                                        storage.upsert(docId, target);
                                    }
                                });
                            });
                });
        mutations.clear();
    }

    protected void applyRestorations() {
        restore.entrySet().parallelStream()
                .forEach(storageDocs -> {
                    final Collection storage = getStorage(storageDocs.getKey());
                    storageDocs.getValue().entrySet().parallelStream()
                            .forEach(idDoc -> {
                                final String docId = idDoc.getKey();
                                final List<IndexEntry> content = idDoc.getValue();
                                if (content == null || content.size() == 0) {
                                    storage.remove(docId);
                                } else {
                                    JsonObject doc = JsonObject.create();
                                    for (IndexEntry entry : content) {
                                        doc.put(entry.field, entry.value);
                                    }
                                    storage.insert(docId, doc);
                                }
                            });
                });
        restore.clear();
    }

    protected String getIndexFullName(String name) {
        return indexNamePrefix + "_" + name;
    }

    protected void upsertFtsIndex(String storeName, Map<String, KeyInformation> keys) {
        final String storeKey = scope.name() + "." + storeName;
        final SearchIndex index = getFtsIndex(storeName);
        final Map<String, Object> params = Optional.ofNullable(index.params()).orElseGet(HashMap::new);
        final Map<String, Object> docConfig =
                (Map<String, Object>) Optional.ofNullable(params.get("doc_config")).orElseGet(HashMap::new);
        final Map<String, Object> mapping =
                (Map<String, Object>) Optional.ofNullable(params.get("mapping")).orElseGet(HashMap::new);
        final Map<String, Object> types =
                (Map<String, Object>) Optional.ofNullable(mapping.get("types")).orElseGet(HashMap::new);
        final Map<String, Object> storeMapping =
                (Map<String, Object>) Optional.ofNullable(types.get(storeKey)).orElseGet(HashMap::new);
        final Map<String, Object> defaultMapping =
                (Map<String, Object>) Optional.ofNullable(mapping.get("default_mapping")).orElseGet(HashMap::new);
        final Map<String, Object> properties =
                (Map<String, Object>) Optional.ofNullable(defaultMapping.get("properties")).orElseGet(HashMap::new);


        index.params(params);
        params.put("mapping", mapping);
        params.put("doc_config", docConfig);
        docConfig.put("mode", "scope.collection.type_field");
        mapping.put("types", types);
        defaultMapping.put("enabled", true);
        mapping.put("default_mapping", defaultMapping);
        defaultMapping.put("properties", properties);
        types.put(storeKey, storeMapping);
        storeMapping.put("dynamic", true);
        storeMapping.put("enabled",true);

        keys.entrySet().forEach(keyDef -> {
            String key = keyDef.getKey();
            KeyInformation keyInfo = keyDef.getValue();
            final Map<String, Object> keyprop =
                    (Map<String, Object>) Optional.ofNullable(properties.get(key)).orElseGet(HashMap::new);
            properties.put(key, keyprop);
            final List<Map<String, Object>> keyFields =
                    (List<Map<String, Object>>) Optional.ofNullable(keyprop.get("fields")).orElseGet(ArrayList::new);

            keyprop.put("dynamic", false);
            keyprop.put("fields", keyFields);

            HashMap<String, Object> keyField = (HashMap<String, Object>) keyFields.parallelStream()
                    .filter(field -> Objects.equals(key, field.get("name")))
                    .findFirst().orElseGet(() -> {
                        HashMap<String, Object> result = new HashMap<>();
                        result.put("name", key);
                        keyFields.add(result);
                        return result;
                    });

            String type = "text";
            Class<?> valueType = keyInfo.getDataType();
            if (Number.class.isAssignableFrom(valueType)) {
                type = "number";
            } else if (valueType == Boolean.class) {
                type = "boolean";
            } else if (valueType == Date.class || valueType == Instant.class){
                type = "datetime";
            }
            keyField.put("type", type);
            keyField.put("index", true);
            keyField.put("store", false);
            keyField.put("include_in_all", false);
            keyField.put("include_term_vectors", false);
            keyField.put("docvalues", false);
        });

        cluster.searchIndexes().upsertIndex(index);
    }

    protected SearchIndex getFtsIndex(String name) {
        String fullName = getIndexFullName(name);
        List<SearchIndex> indexes = null;
        try {
            indexes = cluster.searchIndexes().getAllIndexes();
        } catch (NullPointerException npe) {
            // BUG?
            return createEmptyFtsIndex(name);
        }
        return indexes.stream()
                .filter(index -> fullName.equals(index.name()))
                .findFirst().orElseGet(() -> createEmptyFtsIndex(name));
    }

    protected QueryIndex getIndex(String name) {
        final String fullName = getIndexFullName(name);
        return cluster.queryIndexes().getAllIndexes(bucket.name()).stream()
                .filter(index -> scope.name().equals(index.scopeName().orElse(null)))
                .filter(index -> name.equals(index.collectionName().orElse(null)))
                .filter(index -> fullName.equals(index.name()))
                .findFirst().orElse(null);
    }

    protected Collection getStorage(String name) {
        Collection result = scope.collection(name);
        if (result == null) {
            bucket.collections().createCollection(CollectionSpec.create(name, scope.name()));
            result = scope.collection(name);
        }
        return result;
    }

    protected SearchIndex createEmptyFtsIndex(String name) {
        String fullName = getIndexFullName(name);
        return new SearchIndex(fullName, bucket.name());
    }

    protected QueryIndex updateIndex(String name, Map<String, KeyInformation> keyInfo) {
        String fullName = getIndexFullName(name);
        QueryIndex existing = getIndex(name);
        Set<String> keys = new HashSet<>();
        if (existing != null) {
            cluster.queryIndexes().dropIndex(bucket.name(), fullName,
                    DropQueryIndexOptions.dropQueryIndexOptions()
                            .scopeName(scope.name())
                            .collectionName(name)
                            .ignoreIfNotExists(true));
            existing.indexKey().forEach(k -> keys.add((String) k));
        }

        keyInfo.keySet().stream()
                .map(k -> String.format("`%s`", k))
                .forEach(keys::add);

        cluster.queryIndexes().createIndex(
                bucket.name(),
                fullName,
                keys,
                CreateQueryIndexOptions.createQueryIndexOptions()
                        .scopeName(scope.name())
                        .collectionName(name)
        );

        QueryIndex result = getIndex(name);
        while (result == null) {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            result = getIndex(name);
        }
        cluster.queryIndexes().buildDeferredIndexes(bucket.name(),
                BuildQueryIndexOptions.buildDeferredQueryIndexesOptions()
                        .scopeName(scope.name())
                        .collectionName(name));

        return result;
    }

    @Override
    public BaseTransactionConfig getConfiguration() {
        return config;
    }

    public void restore(Map<String, Map<String, List<IndexEntry>>> documents, KeyInformation.IndexRetriever information) {
        documents.entrySet().forEach(storageDocs -> {
            final String storageName = storageDocs.getKey();
            if (!restore.containsKey(storageName)) {
                restore.put(storageName, new HashMap<>());
            }
            final Map<String, List<IndexEntry>> storageMap = restore.get(storageName);
            storageDocs.getValue().entrySet().forEach(docEntries -> {
                final String docId = docEntries.getKey();
                if (!storageMap.containsKey(docId)) {
                    storageMap.put(docId, new LinkedList<>());
                }
                storageMap.get(docId).addAll(docEntries.getValue());
            });
        });
    }
}
