package org.janusgraph.graphdb.query.condition;

import org.janusgraph.core.JanusElement;
import org.janusgraph.core.JanusRelation;
import org.janusgraph.core.schema.JanusSchemaElement;
import org.janusgraph.core.JanusVertex;
import org.janusgraph.graphdb.internal.InternalElement;
import org.janusgraph.graphdb.types.system.SystemRelationType;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Evaluates elements based on their visibility
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class VisibilityFilterCondition<E extends JanusElement> extends Literal<E> {

    public enum Visibility { NORMAL, SYSTEM }

    private final Visibility visibility;

    public VisibilityFilterCondition(Visibility visibility) {
        this.visibility = visibility;
    }

    @Override
    public boolean evaluate(E element) {
        switch(visibility) {
            case NORMAL: return !((InternalElement)element).isInvisible();
            case SYSTEM: return (element instanceof JanusRelation &&
                                    ((JanusRelation)element).getType() instanceof SystemRelationType)
                    || (element instanceof JanusVertex && element instanceof JanusSchemaElement);
            default: throw new AssertionError("Unrecognized visibility: " + visibility);
        }
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(visibility).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        return this == other || !(other == null || !getClass().isInstance(other));

    }

    @Override
    public String toString() {
        return "visibility:"+visibility.toString().toLowerCase();
    }
}
