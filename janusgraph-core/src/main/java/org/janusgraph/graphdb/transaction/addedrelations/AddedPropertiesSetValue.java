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

import org.apache.tinkerpop.gremlin.structure.Property;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class AddedPropertiesSetValue implements AddedPropertiesValue {

    private final Map<Object, InternalRelation> internalRelationMap;

    public AddedPropertiesSetValue() {
        this.internalRelationMap = new HashMap<>();
    }

    @Override
    public int addValue(InternalRelation internalRelation) {
        int sizeBefore = this.internalRelationMap.size();
        Property prop = (Property) internalRelation;
        this.internalRelationMap.put(prop.value(), internalRelation);
        return this.internalRelationMap.size() - sizeBefore;
    }

    @Override
    public int removeValue(InternalRelation internalRelation) {
        int sizeBefore = this.internalRelationMap.size();
        Property prop = (Property) internalRelation;
        this.internalRelationMap.remove(prop.value());
        return sizeBefore - this.internalRelationMap.size();
    }

    @Override
    public boolean isNull() {
        return false;
    }

    @Override
    public void clear() {
        internalRelationMap.clear();
    }

    @Override
    public Iterator<InternalRelation> getView() {
        return this.internalRelationMap.values().iterator();
    }

    @Override
    public Iterator<InternalRelation> getView(Object value) {
        InternalRelation ir = internalRelationMap.get(value);
        if (ir == null) {
            return Collections.emptyIterator();
        } else {
            return Collections.singletonList(ir).iterator();
        }
    }
}
