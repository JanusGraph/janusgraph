package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.DefaultTypeMaker;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.TypeMaker;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class DisableDefaultTypeMaker implements DefaultTypeMaker {

    public static final DefaultTypeMaker INSTANCE = new DisableDefaultTypeMaker();

    private DisableDefaultTypeMaker() {}

    @Override
    public TitanLabel makeLabel(String name, TypeMaker factory) {
        throw new IllegalArgumentException("Label with given name does not exist: " + name);
    }

    @Override
    public TitanKey makeKey(String name, TypeMaker factory) {
        throw new IllegalArgumentException("Key with given name does not exist: " + name);
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return false;
    }
}
