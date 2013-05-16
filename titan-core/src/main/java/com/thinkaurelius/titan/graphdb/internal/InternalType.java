package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.TitanType;
import com.thinkaurelius.titan.graphdb.types.TypeDefinition;
import com.tinkerpop.blueprints.Direction;

/**
 * Internal Type interface adding methods that should only be used by Titan
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface InternalType extends TitanType, InternalVertex {

    public TypeDefinition getDefinition();

    public boolean isHidden();

    public boolean isStatic(Direction dir);

    public boolean uniqueLock(Direction direction);


}
