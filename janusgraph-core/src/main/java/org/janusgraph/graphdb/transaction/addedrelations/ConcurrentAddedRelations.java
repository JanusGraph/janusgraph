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

import com.google.common.base.Predicate;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Collection;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class ConcurrentAddedRelations extends SimpleAddedRelations {

    @Override
    public synchronized boolean add(final InternalRelation relation) {
        return super.add(relation);
    }

    @Override
    public synchronized boolean remove(final InternalRelation relation) {
        return super.remove(relation);
    }

    @Override
    public synchronized Iterable<InternalRelation> getView(final Predicate<InternalRelation> filter) {
        return super.getView(filter);
    }

    @Override
    public synchronized Collection<InternalRelation> getAll() {
        return super.getAll();
    }
}
