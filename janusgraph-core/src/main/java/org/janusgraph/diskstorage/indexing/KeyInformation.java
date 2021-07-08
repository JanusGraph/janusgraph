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

package org.janusgraph.diskstorage.indexing;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Parameter;

/**
 * Helper class that provides information on the data type and additional parameters that
 * form the definition of a key in an index.
 * <p>
 *
 * So, given a key, its associated KeyInformation provides the details on what data type the key's associated values
 * have and whether this key has been configured with any additional parameters (that might provide information on how
 * the key should be indexed).
 *
 * <p>
 *
 * {@link IndexRetriever} returns {@link KeyInformation} for a given store and given key. This will be provided to an
 * index when the key is not fixed in the context, e.g. in {@link IndexProvider#mutate(java.util.Map, IndexRetriever, org.janusgraph.diskstorage.BaseTransaction)}
 *
 * <p>
 *
 * {@link Retriever} returns {@link IndexRetriever} for a given index identified by its name. This is only used
 * internally to pass {@link IndexRetriever}s around.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface KeyInformation {

    /**
     * Returns the data type of the key's values.
     *
     * @return
     */
    Class<?> getDataType();

    /**
     * Returns the parameters of the key's configuration.
     *
     * @return
     */
    Parameter[] getParameters();

    /**
     * Returns the {@link org.janusgraph.core.Cardinality} for this key.
     * @return
     */
    Cardinality getCardinality();


    interface StoreRetriever {

        /**
         * Returns the {@link KeyInformation} for a particular key for this store
         * @param key
         * @return
         */
        KeyInformation get(String key);

    }

    interface IndexRetriever {

        /**
         * Returns the {@link KeyInformation} for a particular key in a given store.
         *
         * @param store
         * @param key
         * @return
         */
        KeyInformation get(String store, String key);

        /**
         * Returns a {@link StoreRetriever} for the given store on this IndexRetriever
         * @param store
         * @return
         */
        StoreRetriever get(String store);

        void invalidate(String store);
    }

    interface Retriever {

        /**
         * Returns the {@link IndexRetriever} for a given index.
         * @param index
         * @return
         */
        IndexRetriever get(String index);

    }

}
