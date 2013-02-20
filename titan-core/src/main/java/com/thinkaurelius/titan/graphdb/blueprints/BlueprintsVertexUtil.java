package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;
import com.tinkerpop.blueprints.Direction;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BlueprintsVertexUtil {


    public static void setProperty(TitanVertex vertex, TitanTransaction tx, String key, Object value) {
        TitanKey pkey = tx.getPropertyKey(key);
        Preconditions.checkNotNull(pkey);
        if (pkey.isUnique(Direction.OUT)) {
            TitanProperty existing = Iterables.getOnlyElement(vertex.getProperties(pkey), null);
            if (existing != null) existing.remove();
        }
        vertex.addProperty(pkey, value);
    }


}
