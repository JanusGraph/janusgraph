package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.schema.Parameter;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.schema.SchemaStatus;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ParameterIndexField extends IndexField {

    private final Parameter[] parameters;

    private ParameterIndexField(PropertyKey key, Parameter[] parameters) {
        super(key);
        Preconditions.checkNotNull(parameters);
        this.parameters=parameters;
    }

    public SchemaStatus getStatus() {
        SchemaStatus status = ParameterType.STATUS.findParameter(parameters, null);
        Preconditions.checkState(status!=null,"Field [%s] did not have a status",this);
        return status;
    }

    public Parameter[] getParameters() {
        return parameters;
    }

    public static ParameterIndexField of(PropertyKey key, Parameter... parameters) {
        return new ParameterIndexField(key,parameters);
    }


}
