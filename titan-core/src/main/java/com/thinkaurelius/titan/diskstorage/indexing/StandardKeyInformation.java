package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Parameter;

import java.util.HashMap;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardKeyInformation implements KeyInformation {

    private final Class<?> dataType;
    private final Parameter[] parameters;

    public StandardKeyInformation(Class<?> dataType) {
        this(dataType,new Parameter[0]);
    }

    public StandardKeyInformation(Class<?> dataType, Parameter... parameters) {
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(parameters);
        this.dataType = dataType;
        this.parameters = parameters;
    }

    @Override
    public Class<?> getDataType() {
        return dataType;
    }

    public boolean hasParameters() {
        return parameters.length>0;
    }

    @Override
    public Parameter[] getParameters() {
        return parameters;
    }

}
