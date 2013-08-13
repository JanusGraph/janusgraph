package com.thinkaurelius.titan.graphdb.query.condition;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.TitanRelation;
import com.thinkaurelius.titan.core.TitanType;
import org.apache.commons.lang.builder.HashCodeBuilder;

import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */

public class LabelCondition<E extends TitanRelation> extends Literal<E> {

    private final Set<TitanType> labels;

    public LabelCondition(TitanType... labels) {
        this(Sets.newHashSet(labels));
    }

    public LabelCondition(Set<TitanType> labels) {
        Preconditions.checkNotNull(labels);
        Preconditions.checkArgument(!labels.isEmpty());
        this.labels=labels;
    }

    @Override
    public boolean evaluate(E element) {
        return labels.contains(element.getType());
    }

    public Set<TitanType> getLabels() {
        return labels;
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getType()).append(labels).toHashCode();
    }

    @Override
    public boolean equals(Object other) {
        if (this==other) return true;
        else if (other==null) return false;
        else if (!getClass().isInstance(other)) return false;
        LabelCondition oth = (LabelCondition)other;
        return labels.equals(oth.labels);
    }

    @Override
    public String toString() {
        return "label["+labels.toString()+"]";
    }

}
