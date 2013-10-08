package com.thinkaurelius.titan.graphdb.types;

import com.thinkaurelius.titan.core.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class DisableDefaultTypeMaker implements DefaultTypeMaker {

    public static final DefaultTypeMaker INSTANCE = new DisableDefaultTypeMaker();

    private DisableDefaultTypeMaker() {
    }

    @Override
    public TitanLabel makeLabel(LabelMaker factory) {
        throw new IllegalArgumentException("Label with given name does not exist");
    }

    @Override
    public TitanKey makeKey(KeyMaker factory) {
        throw new IllegalArgumentException("Key with given name does not exist");
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return false;
    }
}
