package com.thinkaurelius.titan.diskstorage.indexing;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.internal.OrderList;
import com.thinkaurelius.titan.graphdb.query.BackendQuery;
import com.thinkaurelius.titan.graphdb.query.BaseQuery;
import com.thinkaurelius.titan.graphdb.query.Query;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Comparator;
import java.util.List;

/**
 * An external index query executed on an {@link IndexProvider}.
 * <p/>
 * A query is comprised of the store identifier against which the query ought to be executed and a query condition
 * which defines which entries match the query.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class IndexQuery extends BaseQuery implements BackendQuery<IndexQuery> {

    public static final ImmutableList<OrderEntry> NO_ORDER = ImmutableList.of();

    private final String store;
    private final Condition condition;
    private final ImmutableList<OrderEntry> orders;

    private final int hashcode;

    public IndexQuery(String store, Condition condition, ImmutableList<OrderEntry> orders, int limit) {
        super(limit);
        Preconditions.checkNotNull(store);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(orders != null);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        this.condition = condition;
        this.orders = orders;
        this.store = store;

        this.hashcode = new HashCodeBuilder().append(condition).append(store).append(orders).append(limit).toHashCode();
    }

    public IndexQuery(String store, Condition condition, ImmutableList<OrderEntry> orders) {
        this(store, condition, orders, Query.NO_LIMIT);
    }

    public IndexQuery(String store, Condition condition) {
        this(store, condition, NO_ORDER, Query.NO_LIMIT);
    }

    public IndexQuery(String store, Condition condition, int limit) {
        this(store, condition, NO_ORDER, limit);
    }

    public Condition<TitanElement> getCondition() {
        return condition;
    }

    public List<OrderEntry> getOrder() {
        return orders;
    }

    public String getStore() {
        return store;
    }

    @Override
    public IndexQuery setLimit(int limit) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IndexQuery updateLimit(int newLimit) {
        return new IndexQuery(store, condition, orders, newLimit);
    }

    @Override
    public int hashCode() {
        return hashcode;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) return true;
        else if (other == null) return false;
        else if (!getClass().isInstance(other)) return false;
        IndexQuery oth = (IndexQuery) other;
        return store.equals(oth.store) && orders.equals(oth.orders)
                && condition.equals(oth.condition) && getLimit() == oth.getLimit();
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (!orders.isEmpty()) b.append(orders);
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(store);
        return b.toString();
    }

    public static class OrderEntry {

        private final String key;
        private final Order order;
        private final Class<?> datatype;

        public OrderEntry(String key, Order order, Class<?> datatype) {
            Preconditions.checkNotNull(key);
            Preconditions.checkNotNull(order);
            Preconditions.checkNotNull(datatype);
            this.key = key;
            this.order = order;
            this.datatype = datatype;
        }

        public String getKey() {
            return key;
        }

        public Order getOrder() {
            return order;
        }

        public Class<?> getDatatype() {
            return datatype;
        }

        @Override
        public int hashCode() {
            return key.hashCode() * 4021 + order.hashCode();
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
