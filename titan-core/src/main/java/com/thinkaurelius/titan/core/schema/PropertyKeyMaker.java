package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;

/**
 * Used to define new {@link com.thinkaurelius.titan.core.PropertyKey}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface PropertyKeyMaker extends RelationTypeMaker {


    /**
     * Configures the {@link com.thinkaurelius.titan.core.Cardinality} of this property key.
     * @param cardinality
     * @return
     */
    public PropertyKeyMaker cardinality(Cardinality cardinality);

    /**
     * Configures the data type for this type.  This only applies to property keys.
     * <p/>
     * Property instances for this key will only accept values that are instances of this class.
     * Every property key must have its data type configured. Setting the data type to Object.class allows
     * any type of value but comes at the expense of longer serialization because class information
     * is stored with the value.
     *
     * @param clazz Data type to be configured.
     * @return this type maker
     * @see com.thinkaurelius.titan.core.PropertyKey#getDataType()
     */
    public PropertyKeyMaker dataType(Class<?> clazz);

    @Override
    public PropertyKeyMaker signature(RelationType... types);


    /**
     * Defines the {@link com.thinkaurelius.titan.core.PropertyKey} specified by this KeyMaker and returns the resulting TitanKey
     *
     * @return
     */
    @Override
    public PropertyKey make();
}
