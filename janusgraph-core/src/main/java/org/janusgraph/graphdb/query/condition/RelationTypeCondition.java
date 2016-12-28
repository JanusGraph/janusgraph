package org.janusgraph.graphdb.query.condition;

import com.google.common.base.Preconditions;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.TitanElement;
import org.janusgraph.core.TitanRelation;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 *
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class RelationTypeCondition<E extends TitanElement> extends Literal<E> {

    private final RelationType relationType;

    public RelationTypeCondition(RelationType relationType) {
        Preconditions.checkNotNull(relationType);
        this.relationType = relationType;
    }

    @Override
    public boolean evaluate(E element) {
        Preconditions.checkArgument(element instanceof TitanRelation);
        return relationType.equals(((TitanRelation)element).getType());
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(relationType).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other)) && relationType.equals(((RelationTypeCondition) other).relationType);
    }

    @Override
    public String toString() {
        return "type["+ relationType.toString()+"]";
    }
}
