package com.thinkaurelius.titan.graphdb.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.tinkerpop.gremlin.structure.Edge;
import com.tinkerpop.gremlin.structure.Element;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ElementHelper {

    public static Iterable<Object> getValues(TitanElement element, PropertyKey key) {
        if (element instanceof TitanRelation) {
            Object value = element.valueOrNull(key);
            if (value==null) return Collections.EMPTY_LIST;
            else return ImmutableList.of(value);
        } else {
            assert element instanceof TitanVertex;
            return Iterables.transform((((TitanVertex) element).query()).keys(key.name()).properties(), new Function<TitanVertexProperty, Object>() {
                @Nullable
                @Override
                public Object apply(@Nullable TitanVertexProperty titanProperty) {
                    return titanProperty.value();
                }
            });
        }
    }

    public static long getCompareId(Element element) {
        Object id = element.id();
        if (id instanceof Long) return (Long)id;
        else if (id instanceof RelationIdentifier) return ((RelationIdentifier)id).getRelationId();
        else throw new IllegalArgumentException("Element identifier has unrecognized type: " + id);
    }

    public static void attachProperties(TitanElement element, Object... keyValues) {
        com.tinkerpop.gremlin.structure.util.ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (com.tinkerpop.gremlin.structure.util.ElementHelper.getIdValue(keyValues).isPresent()) throw Edge.Exceptions.userSuppliedIdsNotSupported();
        if (com.tinkerpop.gremlin.structure.util.ElementHelper.getLabelValue(keyValues).isPresent()) throw new IllegalArgumentException("Cannot provide label as argument");
        com.tinkerpop.gremlin.structure.util.ElementHelper.attachProperties(element,keyValues);
    }

    public static Set<String> getPropertyKeys(TitanVertex v) {
        final Set<String> s = new HashSet<>();
        v.query().properties().forEach( p -> s.add(p.propertyKey().name()));
        return s;
    }

}
