package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Parameter;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum Mapping {

    DEFAULT,
    TEXT,
    STRING;

    public static final String MAPPING_PREFIX = "mapping";

    public static Mapping getMapping(KeyInformation information) {
        Mapping mapping = null;
        for (Parameter p : information.getParameters()) {
            if (p.getKey().equalsIgnoreCase(MAPPING_PREFIX)) {
                Object value = p.getValue();
                Preconditions.checkArgument(value!=null && value instanceof Mapping,"Invalid mapping for specified: %s",value);
                Preconditions.checkArgument(mapping==null,"Multiple mappings specified");
                mapping = (Mapping)value;
            }
        }
        if (mapping==null) mapping=DEFAULT;
        return mapping;
    }

    public static Mapping getMapping(String store, String key, KeyInformation.IndexRetriever informations) {
        KeyInformation ki = informations.get(store, key);
        Preconditions.checkArgument(ki!=null,"Could not find key information for: %s",key);
        return getMapping(ki);
    }


}
