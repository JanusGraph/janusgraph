package com.thinkaurelius.titan.core.schema;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;

/**
 * Used to change the default mapping of an indexed key by providing the mapping explicitly as a parameter to
 * {@link PropertyKeyMaker#indexed(String, Class, Parameter[])}.
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


    public static Mapping getMapping(KeyInformation information) {
        Object value = ParameterType.MAPPING.findParameter(information.getParameters(),null);
        if (value==null) return DEFAULT;
        else {
            Preconditions.checkArgument((value instanceof Mapping || value instanceof String),"Invalid mapping specified: %s",value);
            if (value instanceof String) {
                value = Mapping.valueOf(value.toString().toUpperCase());
            }
            return (Mapping)value;
        }
    }

    public static Mapping getMapping(String store, String key, KeyInformation.IndexRetriever informations) {
        KeyInformation ki = informations.get(store, key);
        Preconditions.checkArgument(ki!=null,"Could not find key information for: %s",key);
        return getMapping(ki);
    }


}
