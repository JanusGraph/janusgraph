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

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * This enum is only intended for use by JanusGraph internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardStoreManager {
    BDB_JE("org.janusgraph.diskstorage.berkeleyje.BerkeleyJEStoreManager", "berkeleyje"),
    CQL("org.janusgraph.diskstorage.cql.CQLStoreManager", "cql"),
    HBASE("org.janusgraph.diskstorage.hbase.HBaseStoreManager", "hbase"),
    IN_MEMORY("org.janusgraph.diskstorage.inmemory.InMemoryStoreManager", "inmemory");

    private static final Set<String> ALL_SHORTHANDS;
    private static final Map<String, String> ALL_MANAGER_CLASSES;

    private final String managerClass;
    private final Set<String> shorthands;
    private final String firstStoreShorthand;

    StandardStoreManager(String managerClass, Collection<String> shorthands) {
        if(shorthands == null || shorthands.size() <= 0){
            throw new IllegalArgumentException("At least one shorthand should be specified for manager class "+managerClass);
        }
        this.managerClass = managerClass;
        this.shorthands = Collections.unmodifiableSet(new LinkedHashSet<>(shorthands));
        this.firstStoreShorthand = shorthands.iterator().next();
    }

    StandardStoreManager(String managerClass, String shorthand) {
        this(managerClass, Collections.singleton(shorthand));
    }

    public Set<String> getShorthands() {
        return shorthands;
    }

    public String getFirstStoreShorthand(){
        return firstStoreShorthand;
    }

    public String getManagerClass() {
        return managerClass;
    }

    static {
        StandardStoreManager[] backends = values();
        final Set<String> tempShorthands = new LinkedHashSet<>(backends.length);
        final Map<String, String> tempClassMap = new HashMap<>(backends.length);
        for (final StandardStoreManager backend : backends) {
            backend.getShorthands().forEach(shorthand -> {
                tempShorthands.add(shorthand);
                tempClassMap.put(shorthand, backend.getManagerClass());
            });
        }
        ALL_SHORTHANDS = Collections.unmodifiableSet(tempShorthands);
        ALL_MANAGER_CLASSES = Collections.unmodifiableMap(tempClassMap);
    }

    public static Set<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllManagerClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
