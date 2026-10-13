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

package org.janusgraph.graphdb.internal;

import com.google.common.base.Preconditions;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.PropertyKey;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class OrderList implements Comparator<JanusGraphElement>, Iterable<OrderList.OrderEntry> {

    public static final OrderList NO_ORDER = new OrderList() {{
        makeImmutable();
    }};

    private final List<OrderEntry> list = new ArrayList<>(3);
    private boolean immutable = false;

    public void add(PropertyKey key, Order order) {
        Preconditions.checkArgument(!immutable, "This OrderList has been closed");
        list.add(new OrderEntry(key, order));
    }

    public boolean isEmpty() {
        return list.isEmpty();
    }

    public PropertyKey getKey(int position) {
        return list.get(position).getKey();
    }

    public Order getOrder(int position) {
        return list.get(position).getOrder();
    }

    public int size() {
        return list.size();
    }

    public boolean containsKey(PropertyKey key) {
        for (int i = 0; i < list.size(); i++) if (getKey(i).equals(key)) return true;
        return false;
    }

    public void makeImmutable() {
        this.immutable = true;
    }

    public boolean isImmutable() {
        return immutable;
    }

    /**
     * Whether all individual orders are the same
     *
     * @return
     */
    public boolean hasCommonOrder() {
        Order lastOrder = null;
        for (OrderEntry oe : list) {
            if (lastOrder==null) lastOrder=oe.order;
            else if (lastOrder!=oe.order) return false;
        }
        return true;
    }

    public Order getCommonOrder() {
        Preconditions.checkArgument(hasCommonOrder(),"This OrderList does not have a common order");
        return isEmpty()?Order.DEFAULT:getOrder(0);
    }

    @Override
    public Iterator<OrderEntry> iterator() {
        return list.iterator();
    }

    @Override
    public String toString() {
        return list.toString();
    }

    @Override
    public int hashCode() {
        return list.hashCode();
    }

    @Override
    public boolean equals(Object oth) {
        if (this == oth) return true;
        else if (oth == null) return false;
        else if (!getClass().isInstance(oth)) return false;
        return list.equals(((OrderList) oth).list);
    }

    @Override
    public int compare(JanusGraphElement o1, JanusGraphElement o2) {
        for (OrderEntry aList : list) {
            int cmp = aList.compare(o1, o2);
            if (cmp != 0) return cmp;
        }
//        return o1.compareTo(o2);
        return 0;
    }

    /**
     * Compares two values of an order key in the order of a graph query, which a mixed index sorts the query by when
     * it can: a missing value, {@code null}, comes last in either direction, two strings are compared by code point,
     * and any other values by their natural order. Elasticsearch, OpenSearch, Lucene and Solr sort strings by their
     * UTF-8 bytes, which is the order of their code points, where {@link String#compareTo} compares UTF-16 code units;
     * so the elements a transaction added, and those sorted in memory, come where the index puts the others, as long as
     * the strings are valid UTF-16 and the index's field sorts them as they are, without a normalizer.
     */
    public static int compareOrderValues(Object value1, Object value2, Order order) {
        if (value1 == null) {
            return value2 == null ? 0 : 1;
        } else if (value2 == null) {
            return -1;
        }
        return order.modulateNaturalOrder(value1 instanceof String && value2 instanceof String
            ? compareByCodePoint((String) value1, (String) value2) : ((Comparable) value1).compareTo(value2));
    }

    /*
     * UTF-16 code units order like code points except where, at the first unit in which the strings differ, one has a
     * surrogate (U+D800 to U+DFFF), a unit of a character from U+10000 up, and the other a unit from U+E000 to U+FFFF:
     * the surrogate is the smaller unit, but its character the greater code point. Placing the surrogates after U+FFFF,
     * and the units from U+E000 where the surrogates were, gives the order of the code points. A surrogate without its
     * pair isn't valid UTF-16 and is placed in the same way, which may not be where an index puts it: an index can't
     * keep it as it is, Lucene for one stores U+FFFD instead.
     */
    static int compareByCodePoint(String s1, String s2) {
        final int length = Math.min(s1.length(), s2.length());
        for (int i = 0; i < length; i++) {
            final char c1 = s1.charAt(i);
            final char c2 = s2.charAt(i);
            if (c1 != c2) {
                if (c1 >= Character.MIN_SURROGATE && c2 >= Character.MIN_SURROGATE) {
                    return codePointPlace(c1) - codePointPlace(c2);
                }
                return c1 - c2;
            }
        }
        return s1.length() - s2.length();
    }

    private static int codePointPlace(char unit) {
        return unit > Character.MAX_SURROGATE ? unit - 0x800 : unit + 0x2000;
    }

    /**
     * @author Matthias Broecheler (me@matthiasb.com)
     */

    public static class OrderEntry implements Comparator<JanusGraphElement> {

        private final PropertyKey key;
        private final Order order;

        public OrderEntry(PropertyKey key, Order order) {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(order);
            this.key = key;
            this.order = order;
        }

        public PropertyKey getKey() {
            return key;
        }

        public Order getOrder() {
            return order;
        }

        @Override
        public int hashCode() {
            return key.hashCode() * 1003 + order.hashCode();
        }

        @Override
        public int compare(JanusGraphElement o1, JanusGraphElement o2) {
            return compareOrderValues(o1.valueOrNull(key), o2.valueOrNull(key), order);
        }

        @Override
        public boolean equals(Object oth) {
            if (this == oth) return true;
            else if (oth == null) return false;
            else if (!getClass().isInstance(oth)) return false;
            OrderEntry o = (OrderEntry) oth;
            return key.equals(o.key) && order == o.order;
        }

        @Override
        public String toString() {
            return order + "(" + key + ")";
        }
    }
}
