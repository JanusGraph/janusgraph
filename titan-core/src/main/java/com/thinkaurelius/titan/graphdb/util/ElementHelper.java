package com.thinkaurelius.titan.graphdb.util;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.query.vertex.VertexCentricQueryBuilder;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;

import javax.annotation.Nullable;
import java.util.Collections;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ElementHelper {

    public static final Iterable<Object> getValues(TitanElement element, PropertyKey key) {
        if (element instanceof TitanRelation) {
            Object value = element.getProperty(key);
            if (value==null) return Collections.EMPTY_LIST;
            else return ImmutableList.of(value);
        } else {
            assert element instanceof TitanVertex;
            return Iterables.transform(((VertexCentricQueryBuilder) (((TitanVertex) element).query())).type(key).properties(), new Function<TitanProperty, Object>() {
                @Nullable
                @Override
                public Object apply(@Nullable TitanProperty titanProperty) {
                    return titanProperty.getValue();
                }
            });
        }
    }

}
