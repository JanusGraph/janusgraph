package com.thinkaurelius.titan.diskstorage.indexing;

import com.thinkaurelius.titan.core.Parameter;

/**
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface KeyInformation {


    public Class<?> getDataType();

    public Parameter[] getParameters();


    public interface IndexRetriever {

        public KeyInformation get(String store, String key);

    }

    public interface Retriever {

        public IndexRetriever get(String index);

    }

}
