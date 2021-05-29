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
            Object v1 = o1.valueOrNull(key);
            Object v2 = o2.valueOrNull(key);
            if (v1 == null || v2 == null) {
                if (v1 == null && v2 == null) return 0;
                else if (v1 == null) return 1;
                else return -1; //v2==null
            } else {
                return order.modulateNaturalOrder(((Comparable) v1).compareTo(v2));
            }
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
