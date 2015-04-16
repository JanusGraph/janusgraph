package com.thinkaurelius.titan.core.schema;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Simple class to represent arbitrary parameters as key-value pairs.
 * Parameters are used in configuration and definitions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Parameter<V> {

    private String key;
    private V value;

    public Parameter(String key, V value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key),"Invalid key");
        this.key = key;
        this.value = value;
    }

    public static<V> Parameter<V> of(String key, V value) {
        return new Parameter(key,value);
    }

    public String key() {
        return key;
    }

    public V value() {
        return value;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(key).append(value).toHashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this==oth) return true;
        if (oth==null || !getClass().isInstance(oth)) return false;
        Parameter other = (Parameter)oth;
        return key.equals(other.key) && (value==other.value || (value!=null && value.equals(other.value)));
    }

    @Override
    public String toString() {
        return key+"->"+String.valueOf(value);
    }

}
