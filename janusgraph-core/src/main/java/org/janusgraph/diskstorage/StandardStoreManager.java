// Copyright 2017 JanusGraph Authors
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

package org.janusgraph.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.*;

/**
 * This enum is only intended for use by JanusGraph internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardStoreManager {
    BDB_JE("org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager", "berkeleyje"),
    CASSANDRA_THRIFT("org.janusgraph.diskstorage.cassandra.thrift.CassandraThriftStoreManager", "cassandrathrift"),
    CASSANDRA_ASTYANAX("org.janusgraph.diskstorage.cassandra.astyanax.AstyanaxStoreManager", ImmutableList.of("cassandra", "astyanax")),
    CASSANDRA_EMBEDDED("org.janusgraph.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager", "embeddedcassandra"),
    CQL("org.janusgraph.diskstorage.cql.CQLStoreManager", "cql"),
    HBASE("org.janusgraph.diskstorage.hbase.HBaseStoreManager", "hbase"),
    IN_MEMORY("org.janusgraph.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager", "inmemory");

    private final String managerClass;
    private final ImmutableList<String> shorthands;

    StandardStoreManager(String managerClass, ImmutableList<String> shorthands) {
        this.managerClass = managerClass;
        this.shorthands = shorthands;
    }

    StandardStoreManager(String managerClass, String shorthand) {
        this(managerClass, ImmutableList.of(shorthand));
    }

    public List<String> getShorthands() {
        return shorthands;
    }

    public String getManagerClass() {
        return managerClass;
    }

    private static final ImmutableList<String> ALL_SHORTHANDS;
    private static final ImmutableMap<String, String> ALL_MANAGER_CLASSES;

    static {
        StandardStoreManager[] backends = values();
        final List<String> tempShorthands = new ArrayList<>();
        final Map<String, String> tempClassMap = new HashMap<>();
        for (final StandardStoreManager backend : backends) {
            tempShorthands.addAll(backend.getShorthands());
            for (final String shorthand : backend.getShorthands()) {
                tempClassMap.put(shorthand, backend.getManagerClass());
            }
        }
        ALL_SHORTHANDS = ImmutableList.copyOf(tempShorthands);
        ALL_MANAGER_CLASSES = ImmutableMap.copyOf(tempClassMap);
    }

    public static List<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllManagerClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
