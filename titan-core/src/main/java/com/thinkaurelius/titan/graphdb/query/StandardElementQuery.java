package com.thinkaurelius.titan.graphdb.query;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.query.keycondition.*;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.collections.comparators.ComparableComparator;
import org.apache.commons.lang.StringUtils;

import java.util.Comparator;
import java.util.Iterator;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class StandardElementQuery implements Query<StandardElementQuery> {

    public enum Type {
        VERTEX, EDGE;

        public Class<? extends Element> getElementType() {
            switch(this) {
                case VERTEX: return Vertex.class;
                case EDGE: return Edge.class;
                default: throw new IllegalArgumentException();
            }
        }
    }

    private final KeyCondition<TitanKey> condition;
    private final Type type;
    private final String index;
    private final int limit;

    public StandardElementQuery(Type type, KeyCondition<TitanKey> condition, int limit, String index) {
        Preconditions.checkNotNull(condition);
        Preconditions.checkNotNull(type);
        Preconditions.checkArgument(limit>=0);
        this.condition = condition;
        this.type=type;
        this.index = index;
        this.limit=limit;
    }

    public StandardElementQuery(StandardElementQuery query, String index) {
        Preconditions.checkNotNull(query);
        Preconditions.checkArgument(StringUtils.isNotBlank(index));
        this.condition=query.condition;
        this.type=query.type;
        this.limit=query.limit;
        this.index=index;
    }

    public KeyCondition<TitanKey> getCondition() {
        return condition;
    }

    public Type getType() {
        return type;
    }

    public boolean hasIndex() {
        return index!=null;
    }

    public String getIndex() {
        Preconditions.checkArgument(hasIndex());
        return index;
    }
    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        b.append("[").append(condition.toString()).append("]");
        if (hasLimit()) b.append("(").append(limit).append(")");
        b.append(":").append(type.toString());
        return b.toString();
    }

    @Override
    public int hashCode() {
        return condition.hashCode()*9676463 + type.hashCode()*4711 + limit;
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        StandardElementQuery oth = (StandardElementQuery)other;
        return type==oth.type && condition.equals(oth.condition) && limit==oth.limit;
    }

    @Override
    public boolean hasLimit() {
        return limit!=Query.NO_LIMIT;
    }

    @Override
    public int getLimit() {
        return limit;
    }

    @Override
    public boolean isInvalid() {
        return limit<=0;
    }

    @Override
    public boolean isSorted() {
        return false;
    }

    @Override
    public Comparator getSortOrder() {
        return new ComparableComparator();
    }

    @Override
    public boolean hasUniqueResults() {
        return true;
    }

    public boolean matches(TitanElement element) {
        return matchesCondition(element,condition);
    }

    private static final<T extends TitanType> boolean satisfiesCondition(KeyAtom<T> atom, Object value) {
        return atom.getRelation().satisfiesCondition(value,atom.getCondition());
    }

    public static final<T extends TitanType> boolean matchesCondition(TitanElement element, KeyCondition<T> condition) {
        if (condition instanceof KeyAtom) {
            KeyAtom<T> atom = (KeyAtom<T>) condition;
            T type = atom.getKey();
            if (type.isPropertyKey()) {
                if (type.isUnique(Direction.OUT)) return satisfiesCondition(atom,element.getProperty((TitanKey)type));
                else {
                    Iterator<TitanProperty> iter = ((VertexCentricQueryBuilder)((TitanVertex)element).query()).type(type).includeHidden().properties().iterator();
                    if (iter.hasNext()) {
                        while (iter.hasNext()) {
                            if (satisfiesCondition(atom,iter.next().getValue())) return true;
                        }
                        return false;
                    } else return satisfiesCondition(atom,null);
                }
            } else {
                Preconditions.checkArgument(type.isUnique(Direction.OUT));
                return satisfiesCondition(atom,((TitanRelation) element).getProperty((TitanLabel) type));
            }
        } else if (condition instanceof KeyNot) {
            return !matchesCondition(element, ((KeyNot) condition).getChild());
        } else if (condition instanceof KeyAnd) {
            for (KeyCondition c : ((KeyAnd<T>)condition).getChildren()) {
                if (!matchesCondition(element, c)) return false;
            }
            return true;
        } else if (condition instanceof KeyOr) {
            if (!condition.hasChildren()) return true;
            for (KeyCondition c : ((KeyOr<T>)condition).getChildren()) {
                if (matchesCondition(element, c)) return true;
            }
            return false;
        } else throw new IllegalArgumentException("Invalid condition: " + condition);
    }

}
