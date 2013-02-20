package com.thinkaurelius.titan.graphdb.query;

import com.thinkaurelius.titan.core.TitanElement;

import java.util.Comparator;

/**
 * TODO: add sorting and sort order
 * (c) Matthias Broecheler (me@matthiasb.com)
 */

public interface Query<E> {

    public boolean matches(E element);

    public boolean hasLimit();

    public int getLimit();

    public boolean isSorted();

    public Comparator<E> getSortOrder();

    public boolean hasCustomModifier(String key);

    public Object getCustomModifier(String key);

}
