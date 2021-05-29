// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.util;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.attribute.Cmp;
import org.janusgraph.graphdb.query.graph.GraphCentricQueryBuilder;
import org.janusgraph.graphdb.transaction.StandardJanusGraphTx;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.system.ImplicitKey;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class IndexHelper {
    public static Iterable<? extends Element> getQueryResults(CompositeIndexType index, Object[] values, StandardJanusGraphTx tx) {
        GraphCentricQueryBuilder gb = getQuery(index,values,tx);
        switch(index.getElement()) {
            case VERTEX:
                return gb.vertices();
            case EDGE:
                return gb.edges();
            case PROPERTY:
                return gb.properties();
            default: throw new AssertionError();
        }
    }

    public static GraphCentricQueryBuilder getQuery(CompositeIndexType index, Object[] values, StandardJanusGraphTx tx) {
        Preconditions.checkArgument(index != null && values != null && values.length > 0 && tx != null);
        Preconditions.checkArgument(values.length==index.getFieldKeys().length);
        GraphCentricQueryBuilder gb = tx.query();
        IndexField[] fields = index.getFieldKeys();
        for (int i = 0; i <fields.length; i++) {
            IndexField f = fields[i];
            Object value = values[i];
            Preconditions.checkNotNull(value);
            PropertyKey key = f.getFieldKey();
            Preconditions.checkArgument(key.dataType().equals(value.getClass()),"Incompatible data types for: %s", value);
            gb.has(key, Cmp.EQUAL,value);
        }
        if (index.hasSchemaTypeConstraint()) {
            gb.has(ImplicitKey.LABEL,Cmp.EQUAL,index.getSchemaTypeConstraint().name());
        }
        return gb;
    }
}
