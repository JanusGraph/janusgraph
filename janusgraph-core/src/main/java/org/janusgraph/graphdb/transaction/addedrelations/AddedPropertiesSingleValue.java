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

import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Collections;
import java.util.Iterator;

public class AddedPropertiesSingleValue implements AddedPropertiesValue {

    private InternalRelation internalRelation = null;

    @Override
    public int addValue(InternalRelation internalRelation) {
        this.internalRelation = internalRelation;
        return 1;
    }

    @Override
    public int removeValue(InternalRelation internalRelation) {
        this.internalRelation = null;
        return 1;
    }

    @Override
    public boolean isNull() {
        return this.internalRelation == null;
    }

    @Override
    public void clear() {
        internalRelation = null;
    }

    @Override
    public Iterator<InternalRelation> getView() {
        return Collections.singletonList(this.internalRelation).iterator();
    }

    @Override
    public Iterator<InternalRelation> getView(Object value) {
        if (((JanusGraphVertexProperty) internalRelation).value().equals(value)) {
            return getView();
        } else {
            return Collections.emptyIterator();
        }
    }
}
