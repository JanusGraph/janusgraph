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

package org.janusgraph.graphdb.transaction.addedrelations;

import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleAddedRelations extends ArrayList<InternalRelation> implements AddedRelationsContainer {

    private static final int INITIAL_ADDED_SIZE = 10;

    public SimpleAddedRelations() {
        super(INITIAL_ADDED_SIZE);
    }

    @Override
    public boolean add(InternalRelation relation) {
        return super.add(relation);
    }

    @Override
    public boolean remove(InternalRelation relation) {
        return super.remove(relation);
    }

    @Override
    public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        return this.stream().filter(filter).collect(Collectors.toList());
    }

    @Override
    public Collection<InternalRelation> getAll() {
        return this;
    }
}
