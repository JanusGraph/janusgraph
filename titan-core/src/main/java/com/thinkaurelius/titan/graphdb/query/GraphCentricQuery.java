package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.diskstorage.indexing.IndexQuery;
import com.thinkaurelius.titan.graphdb.internal.ElementType;
import com.thinkaurelius.titan.graphdb.query.condition.Condition;
import com.thinkaurelius.titan.graphdb.query.condition.FixedCondition;
import org.apache.commons.collections.comparators.ComparableComparator;

import java.util.Comparator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class GraphCentricQuery extends BaseQuery implements ElementQuery<TitanElement, IndexQuery> {

    private final Condition<TitanElement> condition;
    private final BackendQueryHolder<IndexQuery> indexQuery;
    private final ElementType type;

    public GraphCentricQuery(ElementType type, Condition<TitanElement> condition, BackendQueryHolder<IndexQuery> indexQuery, int limit) {
        super(limit);
        Preconditions.checkNotNull(condition);
        Preconditions.checkArgument(QueryUtil.isQueryNormalForm(condition));
        Preconditions.checkNotNull(type);
        Preconditions.checkNotNull(indexQuery);
        this.condition = condition;
        this.type=type;
        this.indexQuery=indexQuery;
    }

    public static final GraphCentricQuery emptyQuery() {
        Condition<TitanElement> cond = new FixedCondition<TitanElement>(false);
        return new GraphCentricQuery(ElementType.VERTEX,cond,new BackendQueryHolder<IndexQuery>(new IndexQuery(null,cond),true,null),0);
    }

    public Condition<TitanElement> getCondition() {
        return condition;
    }

    public ElementType getType() {
        return type;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (hasLimit()) b.append("(").append(getLimit()).append(")");
        b.append(":").append(type.toString());
        return b.toString();
    }

    @Override
    public int hashCode() {
        return condition.hashCode()*9676463 + type.hashCode()*4711 + getLimit();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        GraphCentricQuery oth = (GraphCentricQuery)other;
        return type==oth.type && condition.equals(oth.condition) && getLimit()==oth.getLimit();
    }

    @Override
    public boolean isEmpty() {
        return getLimit()<=0;
    }

    @Override
    public int numSubQueries() {
        return 1;
    }

    @Override
    public BackendQueryHolder<IndexQuery> getSubQuery(int position) {
        if (position==0) return indexQuery;
        else throw new IndexOutOfBoundsException();
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public Comparator<TitanElement> getSortOrder() {
        return new ComparableComparator();
    }

    @Override
    public boolean hasDuplicateResults() {
        return false;
    }

    @Override
    public boolean matches(TitanElement element) {
        return condition.evaluate(element);
    }


}
