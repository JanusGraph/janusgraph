package com.thinkaurelius.titan.graphdb.blueprints;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanProperty;
import com.thinkaurelius.titan.core.TitanTransaction;
import com.thinkaurelius.titan.core.TitanVertex;

import java.util.*;

/**
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public class BlueprintsVertexUtil {
    
    public static Set<String> getPropertyKeys(TitanVertex vertex) {
        Set<String> result = new HashSet<String>();
        for (TitanProperty p : vertex.getProperties()) {
            result.add(p.getPropertyKey().getName());
        }
        return result;
    }
    
    public static void setProperty(TitanVertex vertex, TitanTransaction tx, String key, Object value) {
        TitanKey pkey = tx.getPropertyKey(key);
        Preconditions.checkNotNull(pkey);
        if (pkey.isFunctional()) {
            TitanProperty existing = Iterables.getOnlyElement(vertex.getProperties(pkey),null);
            if (existing!=null) existing.remove();
        }
        vertex.addProperty(pkey,value);
    }
    
    public static Object removeProperty(TitanVertex vertex, TitanTransaction tx, String key) {
        Object result = null;
        TitanKey pkey = tx.getPropertyKey(key);
        if (pkey.isFunctional()) {
            TitanProperty existing = Iterables.getOnlyElement(vertex.getProperties(pkey),null);
            if (existing!=null) result = existing.getAttribute();
        } else {
            List<Object> values = new ArrayList<Object>();
            for (TitanProperty p : vertex.getProperties(pkey)) {
                values.add(p.getAttribute());
            }
            result = values;
        }
        Iterator<TitanProperty> piter = vertex.getProperties(pkey).iterator();
        while (piter.hasNext()) {
            piter.next();
            piter.remove();
        }
        return result;
    }
    
}
