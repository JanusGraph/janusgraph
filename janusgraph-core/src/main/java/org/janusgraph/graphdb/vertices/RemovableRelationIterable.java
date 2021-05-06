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

package org.janusgraph.graphdb.vertices;

import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Iterator;

@Deprecated
public class RemovableRelationIterable<O extends JanusGraphRelation>
        implements Iterable<O> {

    private final Iterable<InternalRelation> iterable;

    public RemovableRelationIterable(Iterable<InternalRelation> iterable) {
        this.iterable = iterable;
    }

    @Override
    public Iterator<O> iterator() {
        return new RemovableRelationIterator<>(iterable.iterator());
    }

}
