package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;

/**
 * Used to change the default mapping of an indexed key by providing the mapping explicitly as a parameter to
 * {@link com.thinkaurelius.titan.core.KeyMaker#indexed(String, Class, com.thinkaurelius.titan.core.Parameter[])}.
 *
 * <p/>
 *
 * For instance, to configure that a string be indexed as a whole and not tokenized, pass in the following mapping parameter
 * configuration to the above mentioned method:
 *
 * <pre>
 *     {@code
 *      TitanKey name = graph.makeKey("name").dataType(String.class).indexed("search",Vertex.class,Parameter.of("mapping",Mapping.STRING)).make()
 *     }
 * </pre>
 *
 *
 *
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
