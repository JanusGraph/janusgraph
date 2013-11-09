package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.core.Parameter;

/**
 * Helper class that provides information on the data type and additional parameters that
 * form the definition of a key in an index.
 * <p/>
 *
 * So, given a key, its associated KeyInformation provides the details on what data type the key's associated values
 * have and whether this key has been configured with any additional parameters (that might provide information on how
 * the key should be indexed).
 *
 * <p/>
 *
 * {@link IndexRetriever} returns {@link KeyInformation} for a given store and given key. This will be provided to an
 * index when the key is not fixed in the context, e.g. in {@link IndexProvider#mutate(java.util.Map, com.thinkaurelius.titan.diskstorage.indexing.KeyInformation.IndexRetriever, com.thinkaurelius.titan.diskstorage.TransactionHandle)}
 *
 * <p/>
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
    public Class<?> getDataType();

    /**
     * Returns the parameters of the key's configuration.
     *
     * @return
     */
    public Parameter[] getParameters();


    public interface IndexRetriever {

        /**
         * Returns the {@link KeyInformation} for a particular key in a given store.
         *
         * @param store
         * @param key
         * @return
         */
        public KeyInformation get(String store, String key);

    }

    public interface Retriever {

        /**
         * Returns the {@link IndexRetriever} for a given index.
         * @param index
         * @return
         */
        public IndexRetriever get(String index);

    }

}
