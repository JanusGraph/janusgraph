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
import com.google.common.collect.Sets;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.graphdb.types.CompositeIndexType;
import org.janusgraph.graphdb.types.MixedIndexType;
import org.janusgraph.graphdb.types.IndexField;
import org.janusgraph.graphdb.types.IndexType;
import org.apache.tinkerpop.gremlin.structure.Element;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.RequiredArgsConstructor;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
@RequiredArgsConstructor
public class JanusGraphIndexWrapper implements JanusGraphIndex {

    @Getter(AccessLevel.PACKAGE)
    private final IndexType baseIndex;

    @Override
    public String name() {
        return baseIndex.getName();
    }

    @Override
    public String getBackingIndex() {
        return baseIndex.getBackingIndexName();
    }

    @Override
    public Class<? extends Element> getIndexedElement() {
        return baseIndex.getElement().getElementType();
    }

    @Override
    public PropertyKey[] getFieldKeys() {
        IndexField[] fields = baseIndex.getFieldKeys();
        PropertyKey[] keys = new PropertyKey[fields.length];
        for (int i = 0; i < fields.length; i++) {
            keys[i]=fields[i].getFieldKey();
        }
        return keys;
    }

    @Override
    public Parameter[] getParametersFor(PropertyKey key) {
        if (baseIndex.isCompositeIndex()) return new Parameter[0];
        return ((MixedIndexType)baseIndex).getField(key).getParameters();
    }

    @Override
    public boolean isUnique() {
        if (baseIndex.isMixedIndex()) return false;
        return ((CompositeIndexType)baseIndex).getCardinality()== Cardinality.SINGLE;
    }

    @Override
    public SchemaStatus getIndexStatus(PropertyKey key) {
        Preconditions.checkArgument(Sets.newHashSet(getFieldKeys()).contains(key),"Provided key is not part of this index: %s",key);
        if (baseIndex.isCompositeIndex()) return ((CompositeIndexType)baseIndex).getStatus();
        else return ((MixedIndexType)baseIndex).getField(key).getStatus();
    }

    @Override
    public boolean isCompositeIndex() {
        return baseIndex.isCompositeIndex();
    }

    @Override
    public boolean isMixedIndex() {
        return baseIndex.isMixedIndex();
    }

    @Override
    public String toString() {
        return name();
    }

}

