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

import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class SimpleBufferAddedRelations implements AddedRelationsContainer {

    private static final int INITIAL_ADDED_SIZE = 10;
    private static final int INITIAL_DELETED_SIZE = 10;
    private static final int MAX_DELETED_SIZE = 500;

    private List<InternalRelation> added;
    private List<InternalRelation> deleted;

    public SimpleBufferAddedRelations() {
        added = new ArrayList<>(INITIAL_ADDED_SIZE);
        deleted = null;
    }

    @Override
    public boolean add(InternalRelation relation) {
        return added.add(relation);
    }

    @Override
    public boolean remove(InternalRelation relation) {
        if (added.isEmpty()) return false;
        if (deleted==null) deleted = new ArrayList<>(INITIAL_DELETED_SIZE);
        boolean del = deleted.add(relation);
        if (deleted.size()>MAX_DELETED_SIZE) cleanup();
        return del;
    }

    @Override
    public boolean isEmpty() {
        cleanup();
        return added.isEmpty();
    }

    private void cleanup() {
        if (deleted==null || deleted.isEmpty()) return;
        final Set<InternalRelation> deletedSet = new HashSet<>(deleted);
        deleted=null;
        final List<InternalRelation> newlyAdded = new ArrayList<>(added.size()-deletedSet.size()/2);
        for (InternalRelation r : added) {
            if (!deletedSet.contains(r)) newlyAdded.add(r);
        }
        added=newlyAdded;
    }

    @Override
    public List<InternalRelation> getView(Predicate<InternalRelation> filter) {
        cleanup();
        final List<InternalRelation> result = new ArrayList<>();
        for (InternalRelation r : added) {
            if (filter.apply(r)) result.add(r);
        }
        return result;
    }

    @Override
    public Collection<InternalRelation> getAll() {
        cleanup();
        return added;
    }
}
