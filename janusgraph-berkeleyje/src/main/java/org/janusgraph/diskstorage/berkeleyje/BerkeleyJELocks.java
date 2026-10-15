// Copyright 2026 JanusGraph Authors
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

package org.janusgraph.diskstorage.berkeleyje;

import org.janusgraph.diskstorage.StaticBuffer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * The transactions which hold the locks of the keys of a BerkeleyJE environment's databases. A transaction takes a lock
 * in {@link BerkeleyJEKeyValueStore#acquireLock} and holds it until it commits or rolls back.
 * <p>
 * Only one process at a time can open an environment for writing, and within it, BerkeleyJE opens an environment once
 * for all the handles which open its directory. So the store managers of all the graph instances of a JVM which open
 * the same directory write the same databases, and share these locks: they are kept by the canonical path of the
 * directory, as BerkeleyJE keeps its environments, while a store manager has the environment open.
 */
final class BerkeleyJELocks {

    private static final Map<String, BerkeleyJELocks> OPEN = new HashMap<>();

    private final String directory;
    private final ConcurrentMap<String, ConcurrentMap<StaticBuffer, BerkeleyJETx>> holders = new ConcurrentHashMap<>();
    private int managers;

    private BerkeleyJELocks(String directory) {
        this.directory = directory;
    }

    /**
     * Returns the locks of the environment in the given directory, for a store manager which opened it; the manager
     * calls {@link #close()} when it closes the environment.
     *
     * @param directory the canonical path of the environment's directory
     */
    static BerkeleyJELocks open(String directory) {
        synchronized (OPEN) {
            final BerkeleyJELocks locks = OPEN.computeIfAbsent(directory, BerkeleyJELocks::new);
            locks.managers++;
            return locks;
        }
    }

    void close() {
        synchronized (OPEN) {
            if (--managers == 0) {
                OPEN.remove(directory);
            }
        }
    }

    /**
     * The transactions which hold the locks of the keys of the database of the given name.
     */
    ConcurrentMap<StaticBuffer, BerkeleyJETx> holders(String database) {
        return holders.computeIfAbsent(database, name -> new ConcurrentHashMap<>());
    }
}
