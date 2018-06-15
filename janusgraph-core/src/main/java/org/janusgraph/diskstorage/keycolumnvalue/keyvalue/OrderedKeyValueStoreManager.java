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
import org.janusgraph.diskstorage.keycolumnvalue.StoreTransaction;

import java.util.Map;

/**
 * A {@link KeyValueStoreManager} where the stores maintain keys in their natural order.
 *
 * @author Matthias Br&ouml;cheler (me@matthiasb.com);
 */
public interface OrderedKeyValueStoreManager extends KeyValueStoreManager {

    /**
     * Opens an ordered database by the given name. If the database does not exist, it is
     * created. If it has already been opened, the existing handle is returned.
     * <p>
     *
     * @param name Name of database
     * @return Database Handle
     * @throws org.janusgraph.diskstorage.BackendException
     *
     */
    @Override
    OrderedKeyValueStore openDatabase(String name) throws BackendException;


    /**
     * Executes multiple mutations at once. Each store (identified by a string name) in the mutations map is associated
     * with a {@link KVMutation} that contains all the mutations for that particular store.
     *
     * This is an optional operation. Check {@link #getFeatures()} if it is supported by a particular implementation.
     *
     * @param mutations
     * @param txh
     * @throws org.janusgraph.diskstorage.BackendException
     */
    void mutateMany(Map<String, KVMutation> mutations, StoreTransaction txh) throws BackendException;

}
