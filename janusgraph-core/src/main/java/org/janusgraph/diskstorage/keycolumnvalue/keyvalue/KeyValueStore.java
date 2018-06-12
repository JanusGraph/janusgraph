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

package org.janusgraph.diskstorage.keycolumnvalue.keyvalue;

import org.janusgraph.diskstorage.BackendException;
import org.janusgraph.diskstorage.StaticBuffer;
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

/**
 * Interface for a data store that represents data in the simple key-&gt;value data model where each key is uniquely
 * associated with a value. Keys and values are generic ByteBuffers.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface KeyValueStore {

    /**
     * Deletes the given key from the store.
     *
     * @param key
     * @param txh
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void delete(StaticBuffer key, StoreTransaction txh) throws BackendException;

    /**
     * Returns the value associated with the given key.
     *
     * @param key
     * @param txh
     * @return
     * @throws org.janusgraph.diskstorage.BackendException
     */
    StaticBuffer get(StaticBuffer key, StoreTransaction txh) throws BackendException;

    /**
     * Returns true iff the store contains the given key, else false
     *
     * @param key
     * @param txh
     * @return
     * @throws org.janusgraph.diskstorage.BackendException
     */
    boolean containsKey(StaticBuffer key, StoreTransaction txh) throws BackendException;


    /**
     * Acquires a lock for the given key and expected value (null, if not value is expected).
     *
     * @param key
     * @param expectedValue
     * @param txh
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void acquireLock(StaticBuffer key, StaticBuffer expectedValue, StoreTransaction txh) throws BackendException;

    /**
     * Returns the name of this store
     *
     * @return
     */
    String getName();

    /**
     * Closes this store and releases its resources.
     *
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void close() throws BackendException;

}
