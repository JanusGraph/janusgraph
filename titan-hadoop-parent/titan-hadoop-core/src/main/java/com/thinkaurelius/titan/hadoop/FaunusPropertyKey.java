package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.graphdb.schema.PropertyKeyDefinition;

import javax.annotation.Nullable;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusPropertyKey<T> extends FaunusRelationType implements PropertyKey {

    public static final FaunusPropertyKey<Long> COUNT = new FaunusPropertyKey<Long>(
            new PropertyKeyDefinition(Tokens._COUNT, FaunusElement.NO_ID,Cardinality.SINGLE,Long.class),
            new Function<FaunusElement, Long>() {
                @Nullable
                @Override
                public Long apply(@Nullable FaunusElement element) {
                    if (element instanceof FaunusPathElement)
                        return Long.valueOf(((FaunusPathElement)element).pathCount());
                    else return null;
                }
            }
    );

    private final PropertyKeyDefinition definition;
    private final Function<FaunusElement,T> implicitFunction;

    public FaunusPropertyKey(PropertyKeyDefinition definition, boolean isHidden) {
        super(definition,isHidden);
        this.definition = definition;
        this.implicitFunction = null;
    }

    public FaunusPropertyKey(PropertyKeyDefinition definition, Function<FaunusElement,T> implicitFunction) {
        super(definition,true);
        this.definition = definition;
        this.implicitFunction = implicitFunction;
    }

    @Override
    public Class<?> getDataType() {
        return definition.getDataType();
    }

    @Override
    public Cardinality getCardinality() {
        return definition.getCardinality();
    }

    @Override
    public boolean isPropertyKey() {
        return true;
    }

    @Override
    public boolean isEdgeLabel() {
        return false;
    }

    public boolean isImplicit() {
        return implicitFunction!=null;
    }

    public T computeImplicit(FaunusElement element) {
        Preconditions.checkArgument(isImplicit());
        return implicitFunction.apply(element);
    }
}
