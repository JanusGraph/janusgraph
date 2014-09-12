package com.thinkaurelius.titan.core.schema;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.diskstorage.indexing.KeyInformation;
import com.thinkaurelius.titan.graphdb.types.ParameterType;

/**
 * Used to change the default mapping of an indexed key by providing the mapping explicitly as a parameter to
 * {@link TitanManagement#addIndexKey(TitanGraphIndex, com.thinkaurelius.titan.core.PropertyKey, Parameter[])}.
 * <p/>
 * This applies mostly to string data types of keys, where the mapping specifies whether the string value is tokenized
 * ({@link #TEXT}) or indexed as a whole ({@link #STRING}), or both ({@link #TEXTSTRING}).
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum Mapping {

    DEFAULT,
    TEXT,
    STRING,
    TEXTSTRING;

    /**
     * Returns the mapping as a parameter so that it can be passed to {@link TitanManagement#addIndexKey(TitanGraphIndex, com.thinkaurelius.titan.core.PropertyKey, Parameter[])}
     * @return
     */
    public Parameter getParameter() {
        return ParameterType.MAPPING.getParameter(this);
    }

    //------------ USED INTERNALLY -----------

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
