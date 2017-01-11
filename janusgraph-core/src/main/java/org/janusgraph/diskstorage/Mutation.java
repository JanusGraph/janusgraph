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

package org.janusgraph.diskstorage;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.*;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Container for collection mutations against a data store.
 * Mutations are either additions or deletions.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public abstract class Mutation<E,K> {

    private List<E> additions;

    private List<K> deletions;

    public Mutation(List<E> additions, List<K> deletions) {
        Preconditions.checkNotNull(additions);
        Preconditions.checkNotNull(deletions);
        if (additions.isEmpty()) this.additions=null;
        else this.additions = Lists.newArrayList(additions);
        if (deletions.isEmpty()) this.deletions=null;
        else this.deletions = Lists.newArrayList(deletions);
    }

    public Mutation() {
        this.additions = null;
        this.deletions = null;
    }

    /**
     * Whether this mutation has additions
     * @return
     */
    public boolean hasAdditions() {
        return additions!=null && !additions.isEmpty();
    }

    /**
     * Whether this mutation has deletions
     * @return
     */
    public boolean hasDeletions() {
        return deletions != null && !deletions.isEmpty();
    }

    /**
     * Returns the list of additions in this mutation
     * @return
     */
    public List<E> getAdditions() {
        if (additions==null) return ImmutableList.of();
        return additions;
    }

    /**
     * Returns the list of deletions in this mutation.
     *
     * @return
     */
    public List<K> getDeletions() {
        if (deletions==null) return ImmutableList.of();
        return deletions;
    }

    /**
     * Adds a new entry as an addition to this mutation
     *
     * @param entry
     */
    public void addition(E entry) {
        if (additions==null) additions = new ArrayList<E>();
        additions.add(entry);
    }

    /**
     * Adds a new key as a deletion to this mutation
     *
     * @param key
     */
    public void deletion(K key) {
        if (deletions==null) deletions = new ArrayList<K>();
        deletions.add(key);
    }

    /**
     * Merges another mutation into this mutation. Ensures that all additions and deletions
     * are added to this mutation. Does not remove duplicates if such exist - this needs to be ensured by the caller.
     *
     * @param m
     */
    public void merge(Mutation<E,K> m) {
        Preconditions.checkNotNull(m);

        if (null != m.additions) {
            if (null == additions) additions = m.additions;
            else additions.addAll(m.additions);
        }

        if (null != m.deletions) {
            if (null == deletions) deletions = m.deletions;
            else deletions.addAll(m.deletions);
        }
    }

    public boolean isEmpty() {
        return getTotalMutations()==0;
    }

    public int getTotalMutations() {
        return (additions==null?0:additions.size()) + (deletions==null?0:deletions.size());
    }

    /**
     * Consolidates this mutation by removing redundant deletions. A deletion is considered redundant if
     * it is identical to some addition under the provided conversion functions since we consider additions to apply logically after deletions.
     * Hence, such a deletion would be applied and immediately overwritten by an addition. To avoid this
     * inefficiency, consolidation should be called.
     * </p>
     * The provided conversion functions map additions and deletions into some object space V for comparison.
     * An addition is considered identical to a deletion if both map to the same object (i.e. equals=true) with the respective
     * conversion functions.
     * </p>
     * It needs to be ensured that V objects have valid hashCode() and equals() implementations.
     *
     * @param convertAdds Function which maps additions onto comparison objects.
     * @param convertDels Function which maps deletions onto comparison objects.
     */
    public<V> void consolidate(Function<E,V> convertAdds, Function<K,V> convertDels) {
        if (hasDeletions() && hasAdditions()) {
            Set<V> adds = Sets.newHashSet(Iterables.transform(additions,convertAdds));
            Iterator<K> iter = deletions.iterator();
            while (iter.hasNext()) {
                if (adds.contains(convertDels.apply(iter.next()))) iter.remove();
            }
        }
    }

    public abstract void consolidate();

    /**
     * Checks whether this mutation is consolidated in the sense of {@link #consolidate(com.google.common.base.Function, com.google.common.base.Function)}.
     * This should only be used in assertions and tests due to the performance penalty.
     *
     * @param convertAdds
     * @param convertDels
     * @param <V>
     * @return
     */
    public<V> boolean isConsolidated(Function<E,V> convertAdds, Function<K,V> convertDels) {
        int delBefore = getDeletions().size();
        consolidate(convertAdds,convertDels);
        return getDeletions().size()==delBefore;
    }

    public abstract boolean isConsolidated();



}
