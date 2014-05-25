package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.Parameter;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardKeyInformation implements KeyInformation {

    private final Class<?> dataType;
    private final Parameter[] parameters;
    private final Cardinality cardinality;

    public StandardKeyInformation(Class<?> dataType) {
        this(dataType,new Parameter[0]);
    }

    public StandardKeyInformation(Class<?> dataType, Parameter... parameters) {
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(parameters);
        this.dataType = dataType;
        this.parameters = parameters;
        this.cardinality = Cardinality.SINGLE;
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

    @Override
    public Cardinality getCardinality() {
        return cardinality;
    }

}
