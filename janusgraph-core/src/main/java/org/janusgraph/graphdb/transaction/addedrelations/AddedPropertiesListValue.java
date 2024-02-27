// Copyright 2024 JanusGraph Authors
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

package org.janusgraph.graphdb.transaction.addedrelations;

import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Iterator;
import java.util.LinkedHashSet;

public class AddedPropertiesListValue implements AddedPropertiesValue {

    private final LinkedHashSet<InternalRelation> internalRelationSet;

    public AddedPropertiesListValue() {
        this.internalRelationSet = new LinkedHashSet<>(20);
    }

    @Override
    public int addValue(InternalRelation internalRelation) {
        int sizeBefore = this.internalRelationSet.size();
        this.internalRelationSet.add(internalRelation);
        return this.internalRelationSet.size() - sizeBefore;
    }

    @Override
    public int removeValue(InternalRelation internalRelation) {
        int sizeBefore = this.internalRelationSet.size();
        this.internalRelationSet.remove(internalRelation);
        return sizeBefore - this.internalRelationSet.size();
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public void clear() {
        internalRelationSet.clear();
    }

    @Override
    public Iterator<InternalRelation> getView() {
        return Iterators.transform(this.internalRelationSet.iterator(), e -> {
            assert e != null;
            return e;
        });
    }

    @Override
    public Iterator<InternalRelation> getView(Object value) {
        return Iterables.filter(this::getView, i -> ((JanusGraphVertexProperty) i).value().equals(value)).iterator();
    }
}
