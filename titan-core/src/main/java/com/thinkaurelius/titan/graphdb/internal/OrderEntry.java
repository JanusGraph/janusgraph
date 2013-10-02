package com.thinkaurelius.titan.graphdb.internal;

import com.thinkaurelius.titan.core.Order;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class OrderEntry {

    private final String key;
    private final Order order;

    public OrderEntry(String key, Order order) {
        this.key = key;
        this.order = order;
    }

    public String getKey() {
        return key;
    }

    public Order getOrder() {
        return order;
    }
}
