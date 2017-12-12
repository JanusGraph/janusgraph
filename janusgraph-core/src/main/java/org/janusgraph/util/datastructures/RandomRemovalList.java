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

package org.janusgraph.util.datastructures;

import com.google.common.base.Preconditions;

import java.util.*;

/**
 * A list that allows efficient random removals.
 *
 * @param <T>
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RandomRemovalList<T> implements Collection<T>, Iterator<T> {

    public static final Random random = new Random();

    private List<T> list;
    private final static int numTriesBeforeCompaction = 13;
    private static final double fillFactor = 1.05;
    private int size;
    private int numberOfCompactions;


    private boolean isIterating;


    public RandomRemovalList() {
        this(10);
    }

    public RandomRemovalList(int capacity) {
        list = new ArrayList<>(capacity);
        size = 0;
        numberOfCompactions = 0;
        isIterating = false;
    }

    public RandomRemovalList(Collection<T> objects) {
        list = new ArrayList<>(objects);
        size = objects.size();
        numberOfCompactions = 0;
        isIterating = false;
    }

    @Override
    public boolean add(T obj) {
        Preconditions.checkNotNull(obj, "Random Removal lists only contain non-null elements");
        Preconditions.checkArgument(!isIterating, "Cannot add to a random removal list while it is being iterated over");
        size++;
        return list.add(obj);
    }

    public T getRandom() {
        assert size >= 0;
        if (size == 0) throw new NoSuchElementException("List is empty");
        int numTries = 0;
        T element;
        int pos;
        do {
            pos = random.nextInt(list.size());
            element = list.get(pos);
            numTries++;
        } while (element == null && numTries < numTriesBeforeCompaction);
        if (element != null) {
            list.set(pos, null);
            size--;
            return element;
        } else {
            //Compact list
            final List<T> newList = new ArrayList<>((int) Math.ceil(fillFactor * size));
            for (T obj : list) {
                if (obj != null) newList.add(obj);
            }
            list = newList;
            numberOfCompactions++;
            return getRandom();
        }
    }

    @Override
    public int size() {
        assert size >= 0;
        return size;
    }

    @Override
    public boolean isEmpty() {
        assert size >= 0;
        return size == 0;
    }

    public int getNumCompactions() {
        return numberOfCompactions;
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        boolean ret = true;
        for (T obj : c) {
            if (!add(obj)) ret = false;
        }
        return ret;
    }


    @Override
    public void clear() {
        list.clear();
        size = 0;
        numberOfCompactions = 0;
    }

    @Override
    public boolean contains(Object o) {
        return o != null && list.contains(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        boolean ret = true;
        for (Object obj : c) {
            if (!contains(obj)) ret = false;
        }
        return ret;
    }

    @Override
    public boolean hasNext() {
        isIterating = true;
        return !isEmpty();
    }

    @Override
    public T next() {
        isIterating = true;
        return getRandom();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException("Element has already been removed");
    }

    @Override
    public Iterator<T> iterator() {
        return this;
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object[] toArray() {
        throw new UnsupportedOperationException();
    }

    @Override
    public <E> E[] toArray(E[] a) {
        throw new UnsupportedOperationException();
    }


}
