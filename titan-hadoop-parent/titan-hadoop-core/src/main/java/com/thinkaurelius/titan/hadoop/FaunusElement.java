package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;
import com.thinkaurelius.titan.core.TitanElement;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement implements InternalElement, Comparable<FaunusElement> {


    protected static final Predicate<FaunusProperty> FILTER_DELETED_PROPERTIES = new Predicate<FaunusProperty>() {
        @Override
        public boolean apply(@Nullable FaunusProperty p) {
            return !p.isRemoved();
        }
    };
    protected static final Predicate<StandardFaunusEdge> FILTER_DELETED_EDGES = new Predicate<StandardFaunusEdge>() {
        @Override
        public boolean apply(@Nullable StandardFaunusEdge e) {
            return !e.isRemoved();
        }
    };

    public static final long NO_ID = -1;
    static final Multimap<FaunusRelationType, FaunusRelation> EMPTY_ADJACENCY = ImmutableListMultimap.of();

    protected long id;
    protected Multimap<FaunusRelationType, FaunusRelation> adjacency = EMPTY_ADJACENCY;
    protected byte lifecycle = ElementLifeCycle.New;

    public FaunusElement(final long id) {
        this.id = id;
    }

    protected abstract FaunusTypeManager getTypeManager();

    @Override
    public InternalElement it() {
        return this;
    }

    @Override
    public StandardTitanTx tx() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() throws UnsupportedOperationException {
        lifecycle = ElementLifeCycle.Removed;
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getId() {
        return this.id;
    }

    @Override
    public long getLongId() {
        return this.id;
    }

    @Override
    public boolean hasId() {
        return id>=0;
    }

    @Override
    public void setId(final long id) {
        Preconditions.checkArgument(id>=0);
        this.id = id;
    }

    void updateSchema(HadoopSerializer.Schema schema) {
        schema.addAll(adjacency.keySet());
    }

    public void updateLifeCycle(ElementLifeCycle.Event event) {
        this.lifecycle = ElementLifeCycle.update(lifecycle,event);
    }

    public void setLifeCycle(byte lifecycle) {
        Preconditions.checkArgument(ElementLifeCycle.isValid(lifecycle));
        this.lifecycle = lifecycle;
    }

    @Override
    public byte getLifeCycle() {
        return lifecycle;
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    @Override
    public boolean isNew() {
        return ElementLifeCycle.isNew(lifecycle);
    }

    @Override
    public boolean isRemoved() {
        return ElementLifeCycle.isRemoved(lifecycle);
    }

    @Override
    public boolean isLoaded() {
        return ElementLifeCycle.isLoaded(lifecycle);
    }

    public boolean isModified() {
        if (ElementLifeCycle.isModified(lifecycle)) return true;
        for (FaunusRelation r : adjacency.values()) {
            if (r.isModified()) return true;
        }
        return false;
    }

    //##################################
    // Property Handling
    //##################################

    protected void initializeAdjacency() {
        if (this.adjacency == EMPTY_ADJACENCY) adjacency = HashMultimap.create();
    }

    protected Multiplicity getAdjustedMultiplicity(FaunusRelationType type) {
        if (this instanceof FaunusRelation) {
            return Multiplicity.MANY2ONE;
        } return type.getMultiplicity();
    }

    protected void setRelation(final FaunusRelation relation) {
        Preconditions.checkArgument(getAdjustedMultiplicity(relation.getType())==Multiplicity.MANY2ONE);
        //Mark all existing ones for the type as deleted
        final Iterator<FaunusRelation> rels = adjacency.get(relation.getType()).iterator();
        while (rels.hasNext()) {
            FaunusRelation r = rels.next();
            if (r.isNew()) rels.remove();
            r.updateLifeCycle(ElementLifeCycle.Event.REMOVED);
        }
        addRelation(relation);
    }

    protected FaunusRelation addRelation(final FaunusRelation relation) {
        Preconditions.checkNotNull(relation);
        initializeAdjacency();
        if ((this instanceof HadoopVertex) && adjacency.containsEntry(relation.getType(), relation)) {
            //First, check if this relation already exists; if so, consolidate
            final FaunusRelation existing = Iterables.getOnlyElement(Iterables.filter(adjacency.get(relation.getType()),
                    new Predicate<FaunusRelation>() {
                @Override
                public boolean apply(@Nullable FaunusRelation rel) {
                    return relation.equals(rel);
                }
            }));
            StandardFaunusRelation old = (StandardFaunusRelation)existing;
            if (relation.isNew() && old.isRemoved()) {
                old.setLifeCycle(ElementLifeCycle.Loaded);
            } else if (relation.isLoaded() && old.isNew()) {
                old.setLifeCycle(ElementLifeCycle.Loaded);
            }
            return old;
        } else {
            //Verify multiplicity constraint
            switch(relation.getType().getMultiplicity()) {
                case MANY2ONE:
                case ONE2ONE:
                    for (FaunusRelation rel : adjacency.get(relation.getType())) {
                        if (!rel.isRemoved()) throw new IllegalArgumentException("A relation already exists which" +
                                "violates the multiplicity constraint: " + relation.getType().getMultiplicity());
                    }
                    break;
                case SIMPLE:
                    for (FaunusRelation rel : adjacency.get(relation.getType())) {
                        if (rel.isRemoved()) continue;
                        if (relation.isEdge()) {
                            FaunusEdge e1 = (FaunusEdge)relation, e2 = (FaunusEdge)rel;
                            if (e1.getVertex(Direction.OUT).equals(e2.getVertex(Direction.OUT)) &&
                                    e1.getVertex(Direction.IN).equals(e2.getVertex(Direction.IN))) {
                                throw new IllegalArgumentException("A relation already exists which" +
                                        "violates the multiplicity constraint: " + relation.getType().getMultiplicity());
                            }
                        } else {
                            FaunusProperty p1 = (FaunusProperty)relation, p2 = (FaunusProperty)rel;
                            if (p1.getValue().equals(p2.getValue())) {
                                throw new IllegalArgumentException("A relation already exists which" +
                                        "violates the multiplicity constraint: " + relation.getType().getMultiplicity());
                            }
                        }
                        if (!rel.isRemoved()) throw new IllegalArgumentException("A relation already exists which" +
                                "violates the multiplicity constraint: " + relation.getType().getMultiplicity());
                    }
                    break;
                case MULTI:
                case ONE2MANY:
                    //Nothing to check
                    break;
                default: throw new AssertionError();
            }
            adjacency.put(relation.getType(), relation);
            return relation;
        }
    }

    @Override
    public void setProperty(final String key, final Object value) {
        setProperty(getTypeManager().getPropertyKey(key),value);
    }

    @Override
    public <T> T removeProperty(final String key) {
        return removeProperty(getTypeManager().getPropertyKey(key));
    }

    @Override
    public <O> O removeProperty(RelationType type) {
        if (adjacency.isEmpty()) return null;
        FaunusPropertyKey key = (FaunusPropertyKey)type;
        final List<FaunusProperty> removed = Lists.newArrayList();
        final Iterator<FaunusRelation> props = adjacency.get(key).iterator();
        while (props.hasNext()) {
            FaunusProperty p = (FaunusProperty)props.next();
            if (!p.isRemoved()) removed.add(p);
            if (p.isNew()) props.remove();
            else p.updateLifeCycle(ElementLifeCycle.Event.REMOVED);
        }
        if (removed.isEmpty()) return null;
        else if (getAdjustedMultiplicity(key)==Multiplicity.MANY2ONE) return (O)removed.iterator().next().getValue();
        else return (O) removed;
    }

    @Override
    public <T> T getProperty(PropertyKey key) {
        FaunusPropertyKey type = (FaunusPropertyKey)key;
        if (type.isImplicit()) return (T)type.computeImplicit(this);

        Object result = null;
        for (final FaunusRelation p : adjacency.get(type)) {
            if (p.isRemoved()) continue;
            if (result != null)
                throw new IllegalStateException("Use getProperties(String) method for multi-valued properties");
            result = ((FaunusProperty)p).getValue();
        }
        return (T)result;
    }

    @Override
    public <T> T getProperty(final String key) {
        return getProperty(getTypeManager().getPropertyKey(key));
    }

    @Override
    public Set<String> getPropertyKeys() {
        return Sets.newHashSet(Iterables.transform(getPropertyKeysDirect(),new Function<RelationType, String>() {
            @Nullable
            @Override
            public String apply(@Nullable RelationType relationType) {
                return relationType.getName();
            }
        }));
    }

    protected Iterable<RelationType> getPropertyKeysDirect() {
        final Set<RelationType> result = Sets.newHashSet();
        for (final FaunusRelation r : adjacency.values()) {
            if (r.isEdge() && (this instanceof HadoopVertex)) continue;
            if (!r.isRemoved() && !r.getType().isHidden()) result.add(r.getType());
        }
        return result;
    }




    public Collection<FaunusProperty> getProperties() {
        final List<FaunusProperty> result = Lists.newArrayList(Iterables.filter(adjacency.values(), new Predicate<FaunusProperty>() {
            @Override
            public boolean apply(@Nullable FaunusProperty property) {
                return !property.getType().isHidden() && !property.isRemoved();
            }
        }));
        return result;
    }

    public void addAllProperties(final Iterable<FaunusProperty> properties) {
        for (final FaunusProperty p : properties) addProperty(p);
    }

    public Collection<FaunusProperty> getPropertiesWithState() {
        return this.adjacency.values();
    }

    //##################################
    // General Utility
    //##################################

    @Override
    public boolean equals(final Object other) {
        if (this==other) return true;
        else if (other==null || !(other instanceof TitanElement)) return false;
        TitanElement o = (TitanElement)other;
        if (!hasId() || !o.hasId()) return o==this;
        if (getLongId()!=o.getLongId()) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return ((Long) this.id).hashCode();
    }

    @Override
    public int compareTo(FaunusElement o) {
        return new Long(this.id).compareTo((Long) o.getId());
    }
}
