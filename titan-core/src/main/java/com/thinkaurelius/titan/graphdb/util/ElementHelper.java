package com.thinkaurelius.titan.graphdb.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ElementHelper {

    public static Iterable<Object> getValues(TitanElement element, PropertyKey key) {
        if (element instanceof TitanRelation) {
            Object value = element.getProperty(key);
            if (value==null) return Collections.EMPTY_LIST;
            else return ImmutableList.of(value);
        } else {
            assert element instanceof TitanVertex;
            return Iterables.transform((((TitanVertex) element).query()).keys(key.name()).properties(), new Function<TitanVertexProperty, Object>() {
                @Nullable
                @Override
                public Object apply(@Nullable TitanVertexProperty titanProperty) {
                    return titanProperty.getValue();
                }
            });
        }
    }

    public static void attachProperties(TitanElement element, Object... keyValues) {
        com.tinkerpop.gremlin.structure.util.ElementHelper.legalPropertyKeyValueArray(keyValues);
        com.tinkerpop.gremlin.structure.util.ElementHelper.attachProperties(element,keyValues);
    }

}
