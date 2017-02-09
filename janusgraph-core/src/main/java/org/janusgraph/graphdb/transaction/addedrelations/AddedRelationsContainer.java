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
import com.google.common.collect.ImmutableList;
import org.janusgraph.graphdb.internal.InternalRelation;

import java.util.Collection;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public interface AddedRelationsContainer {

    public boolean add(InternalRelation relation);

    public boolean remove(InternalRelation relation);

    public List<InternalRelation> getView(Predicate<InternalRelation> filter);

    public boolean isEmpty();

    /**
     * This method returns all relations in this container. It may only be invoked at the end
     * of the transaction after there are no additional changes. Otherwise the behavior is non deterministic.
     * @return
     */
    public Collection<InternalRelation> getAll();


    public static final AddedRelationsContainer EMPTY = new AddedRelationsContainer() {
        @Override
        public boolean add(InternalRelation relation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(InternalRelation relation) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
            return ImmutableList.of();
        }

        @Override
        public boolean isEmpty() {
            return true;
        }

        @Override
        public Collection<InternalRelation> getAll() {
            return ImmutableList.of();
        }
    };

}
