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

package org.janusgraph.graphdb.database.management;

import com.google.common.base.Preconditions;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.janusgraph.graphdb.types.MixedIndexType;

import java.util.Objects;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class JanusGraphIndexWrapper implements JanusGraphIndex {

    private final IndexType index;

    public JanusGraphIndexWrapper(IndexType index) {
        this.index = index;
    }

    IndexType getBaseIndex() {
        return index;
    }

    @Override
    public String name() {
        return index.getName();
    }

    @Override
    public String getBackingIndex() {
        return index.getBackingIndexName();
    }

    @Override
    public Class<? extends Element> getIndexedElement() {
        return index.getElement().getElementType();
    }

    @Override
    public PropertyKey[] getFieldKeys() {
        IndexField[] fields = index.getFieldKeys();
        PropertyKey[] keys = new PropertyKey[fields.length];
        for (int i = 0; i < fields.length; i++) {
            keys[i]=fields[i].getFieldKey();
        }
        return keys;
    }

    @Override
    public Parameter[] getParametersFor(PropertyKey key) {
        if (index.isCompositeIndex()) return new Parameter[0];
        return ((MixedIndexType)index).getField(key).getParameters();
    }

    @Override
    public boolean isUnique() {
        return !index.isMixedIndex() && ((CompositeIndexType) index).getCardinality() == Cardinality.SINGLE;
    }

    @Override
    public SchemaStatus getIndexStatus(PropertyKey key) {
        Preconditions.checkArgument(containsPropertyKey(key),"Provided key is not part of this index: %s",key);
        if (index.isCompositeIndex()) return ((CompositeIndexType)index).getStatus();
        else return ((MixedIndexType)index).getField(key).getStatus();
    }

    @Override
    public boolean isCompositeIndex() {
        return index.isCompositeIndex();
    }

    @Override
    public boolean isMixedIndex() {
        return index.isMixedIndex();
    }

    @Override
    public String toString() {
        return name();
    }

    private boolean containsPropertyKey(PropertyKey propertyKey){
        for (IndexField field : index.getFieldKeys()) {
            if (Objects.equals(field.getFieldKey(), propertyKey)) {
                return true;
            }
        }
        return false;
    }
}

