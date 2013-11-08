package com.thinkaurelius.titan.core;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Simple class to represent an arbitrary parameters
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class Parameter<V> {

    private String key;
    private V value;

    private Parameter() {} //For serialization

    public Parameter(String key, V value) {
        Preconditions.checkArgument(StringUtils.isNotBlank(key),"Invalid key");
        this.key = key;
        this.value = value;
    }

    public static<V> Parameter<V> of(String key, V value) {
        return new Parameter(key,value);
    }

    public String getKey() {
        return key;
    }

    public V getValue() {
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
        return "("+key+":"+value+")";
    }

}
