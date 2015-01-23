package com.thinkaurelius.titan.diskstorage;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.*;

/**
 * This enum is only intended for use by Titan internals.
 * It is subject to backwards-incompatible change.
 */
public enum StandardStoreManager {
    BDB_JE("com.thinkaurelius.titan.diskstorage.berkeleyje.BerkeleyJEStoreManager", "berkeleyje"),
    CASSANDRA_THRIFT("com.thinkaurelius.titan.diskstorage.cassandra.thrift.CassandraThriftStoreManager", "cassandrathrift"),
    CASSANDRA_ASTYANAX("com.thinkaurelius.titan.diskstorage.cassandra.astyanax.AstyanaxStoreManager", ImmutableList.of("cassandra", "astyanax")),
    CASSANDRA_EMBEDDED("com.thinkaurelius.titan.diskstorage.cassandra.embedded.CassandraEmbeddedStoreManager", "embeddedcassandra"),
    HBASE("com.thinkaurelius.titan.diskstorage.hbase.HBaseStoreManager", "hbase"),
    IN_MEMORY("com.thinkaurelius.titan.diskstorage.keycolumnvalue.inmemory.InMemoryStoreManager", "inmemory");

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

    private static final List<String> ALL_SHORTHANDS;
    private static final Map<String, String> ALL_MANAGER_CLASSES;

    static {
        StandardStoreManager backends[] = values();
        List<String> shorthandList = new ArrayList<String>();
        Map<String, String> shorthandClassMap = new HashMap<String, String>();
        for (int i = 0; i < backends.length; i++) {
            shorthandList.addAll(backends[i].getShorthands());
            for (String shorthand : backends[i].getShorthands()) {
                shorthandClassMap.put(shorthand, backends[i].getManagerClass());
            }
        }
        ALL_SHORTHANDS = shorthandList;
        ALL_MANAGER_CLASSES = shorthandClassMap;
    }

    public static List<String> getAllShorthands() {
        return ALL_SHORTHANDS;
    }

    public static Map<String, String> getAllManagerClasses() {
        return ALL_MANAGER_CLASSES;
    }
}
