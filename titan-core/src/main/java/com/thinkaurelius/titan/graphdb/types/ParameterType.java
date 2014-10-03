package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.schema.Parameter;
import org.apache.commons.lang.StringUtils;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public enum ParameterType {

    MAPPING("mapping"), INDEX_POSITION("index-pos"), MAPPED_NAME("mapped-name"), STATUS("status");

    private final String name;

    private ParameterType(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name));
        this.name=name;
    }

    @Override
    public String toString() {
        return name;
    }

    public String getName() {
        return name;
    }

    public<V> V findParameter(Parameter[] parameters, V defaultValue) {
        V result = null;
        for (Parameter p : parameters) {
            if (p.getKey().equalsIgnoreCase(name)) {
                Object value = p.getValue();
                Preconditions.checkArgument(value!=null,"Invalid mapping specified: %s",value);
                Preconditions.checkArgument(result==null,"Multiple mappings specified");
                result = (V)value;
            }
        }
        if (result==null) return defaultValue;
        return result;
    }

    public boolean hasParameter(Parameter[] parameters) {
        return findParameter(parameters,null)!=null;
    }

    public<V> Parameter<V> getParameter(V value) {
        return new Parameter<V>(name,value);
    }



}

