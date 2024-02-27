// Copyright 2022 JanusGraph Authors
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
import java.util.Collections;
import java.util.List;

public class EmptyAddedRelations implements AddedRelationsContainer {

    private static final EmptyAddedRelations INSTANCE = new EmptyAddedRelations();

    private EmptyAddedRelations() {
    }

    public static EmptyAddedRelations getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean add(InternalRelation relation) {
        return false;
    }

    @Override
    public boolean remove(InternalRelation relation) {
        return false;
    }

    @Override
    public Iterable<InternalRelation> getView(Predicate<InternalRelation> filter) {
        return Collections.emptyList();
    }

    @Override
    public Iterable<InternalRelation> getViewOfProperties(Predicate<InternalRelation> filter) {
        return Collections.emptyList();
    }

    @Override
    public List<InternalRelation> getViewOfProperties(String... keys) {
        return Collections.emptyList();
    }

    @Override
    public List<InternalRelation> getViewOfProperties(String key, Object value) {
        return Collections.emptyList();
    }

    @Override
    public Iterable<InternalRelation> getViewOfPreviousRelations(long id) {
        return Collections.emptyList();
    }

    @Override
    public boolean isEmpty() {
        return true;
    }

    @Override
    public Collection<InternalRelation> getAllUnsafe() {
        return Collections.emptyList();
    }

    @Override
    public void clear() {
    }
}
