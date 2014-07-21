package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.google.common.primitives.Longs;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.ElementLifeCycle;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.tinkerpop.blueprints.Direction;

import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class FaunusElement extends LifeCycleElement implements InternalElement, Comparable<FaunusElement> {


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

    private static final Logger log =
            LoggerFactory.getLogger(FaunusElement.class);

    public static final long NO_ID = -1;
    static final SetMultimap<FaunusRelationType, FaunusRelation> EMPTY_ADJACENCY = ImmutableSetMultimap.of();

    protected long id;
    protected SetMultimap<FaunusRelationType, FaunusRelation> outAdjacency = EMPTY_ADJACENCY;
    protected SetMultimap<FaunusRelationType, FaunusRelation> inAdjacency = EMPTY_ADJACENCY;

    public FaunusElement(final long id) {
        this.id = id;
    }

    public abstract FaunusTypeManager getTypeManager();

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

    void updateSchema(FaunusSerializer.Schema schema) {
        schema.addAll(inAdjacency.keySet());
        schema.addAll(outAdjacency.keySet());
    }

    @Override
    public boolean isHidden() {
        return false;
    }

    public boolean isModified() {
        if (super.isModified()) return true;
        if (!(this instanceof FaunusVertex)) return false;
        for (Direction dir : Direction.proper) {
            for (FaunusRelation r : getAdjacency(dir).values()) {
                if (r.isModified()) return true;
            }
        }
        return false;
    }

    //##################################
    // General Relation Handling
    //##################################

    protected Multiplicity getAdjustedMultiplicity(FaunusRelationType type) {
        if (this instanceof FaunusRelation) {
            return Multiplicity.MANY2ONE;
        } return type.getMultiplicity();
    }

    SetMultimap<FaunusRelationType, FaunusRelation> getAdjacency(Direction dir) {
        assert dir==Direction.IN || dir==Direction.OUT;
        if (dir==Direction.IN) return inAdjacency;
        else return outAdjacency;
    }

    protected void initializeAdjacency(Direction dir) {
        if ((dir==Direction.OUT || dir==Direction.BOTH) && this.outAdjacency == EMPTY_ADJACENCY)
            outAdjacency = HashMultimap.create();
        if ((dir==Direction.IN || dir==Direction.BOTH) && this.inAdjacency == EMPTY_ADJACENCY)
            inAdjacency = HashMultimap.create();
    }

    protected void setRelation(final FaunusRelation relation) {
        int killedRels = 0;
        final Iterator<FaunusRelation> rels = outAdjacency.get(relation.getType()).iterator();
        while (rels.hasNext()) {
            FaunusRelation r = rels.next();
            if (r.isNew()) rels.remove();
            r.updateLifeCycle(ElementLifeCycle.Event.REMOVED);
            updateLifeCycle(ElementLifeCycle.Event.REMOVED_RELATION);
            killedRels++;
        }

        final Multiplicity adjMulti = getAdjustedMultiplicity(relation.getType());

        if (adjMulti != Multiplicity.MANY2ONE && 0 < killedRels) {
            // Calling setRelation on a multi-valued type will delete any
            // existing relations of that type, no matter how many -- log this
            // behavior and suggest addRelation to suppress the warning when
            // using a multi-valued type
            log.info( "setRelation deleted {} relations of type {} with multiplicity {}; " +
                      "use addRelation instead of setRelation to avoid deletion",
                      killedRels, relation.getType(), adjMulti);
        }

        addRelation(relation);
    }

    protected FaunusRelation addRelation(final FaunusRelation relation) {
        Preconditions.checkNotNull(relation);
        FaunusRelation old = null;
        for (Direction dir : Direction.proper) {
            //Determine applicable directions
            if (relation.isProperty() && dir==Direction.IN) {
                continue;
            } else if (relation.isEdge()) {
                FaunusEdge edge = (FaunusEdge)relation;
                if (edge.getEdgeLabel().isUnidirected()) {
                    if (dir==Direction.IN) continue;
                } else if (!edge.getVertex(dir).equals(this)) {
                    continue;
                }
            }

            initializeAdjacency(dir);
            SetMultimap<FaunusRelationType, FaunusRelation> adjacency = getAdjacency(dir);
            if ((this instanceof FaunusVertex) && adjacency.containsEntry(relation.getType(), relation)) {
                //First, check if this relation already exists; if so, consolidate
                old = Iterables.getOnlyElement(Iterables.filter(adjacency.get(relation.getType()),
                        new Predicate<FaunusRelation>() {
                    @Override
                    public boolean apply(@Nullable FaunusRelation rel) {
                        return relation.equals(rel);
                    }
                }));
                if (relation.isNew() && old.isRemoved()) {
                    old.setLifeCycle(ElementLifeCycle.Loaded);
                    updateLifeCycle(ElementLifeCycle.Event.ADDED_RELATION);
                } else if (relation.isLoaded() && old.isNew()) {
                    old.setLifeCycle(ElementLifeCycle.Loaded);
                }
            } else {
                //Verify multiplicity constraint
                switch(relation.getType().getMultiplicity()) {
                    case MANY2ONE:
                        if (dir==Direction.OUT)
                            ensureUniqueness(relation.getType(),adjacency);
                        break;
                    case ONE2MANY:
                        if (dir==Direction.IN)
                            ensureUniqueness(relation.getType(),adjacency);
                        break;
                    case ONE2ONE:
                        ensureUniqueness(relation.getType(),adjacency);
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
                        }
                        break;
                    case MULTI: //Nothing to check
                        break;
                    default: throw new AssertionError();
                }
                adjacency.put(relation.getType(), relation);
                updateLifeCycle(ElementLifeCycle.Event.ADDED_RELATION);
                log.trace("Added relation {} to {}", relation, this);
            }
        }
        if (old!=null) return old;
        else return relation;
    }

    private static void ensureUniqueness(FaunusRelationType type, SetMultimap<FaunusRelationType, FaunusRelation> adjacency) {
        for (FaunusRelation rel : adjacency.get(type)) {
            if (!rel.isRemoved()) throw new IllegalArgumentException("A relation already exists which " +
                    "violates the multiplicity constraint: " + type.getMultiplicity() + " on type " + type);
        }
    }

    public abstract FaunusVertexQuery query();

    //##################################
    // Property Handling
    //##################################


    public void setProperty(EdgeLabel label, TitanVertex vertex) {
        setProperty((FaunusRelationType)label,vertex);
    }

    @Override
    public void setProperty(PropertyKey key, Object value) {
        setProperty((FaunusRelationType)key,value);
    }

    @Override
    public void setProperty(final String key, final Object value) {
        FaunusRelationType rt = getTypeManager().getRelationType(key);
        if (rt==null) rt = getTypeManager().getPropertyKey(key);
        setProperty(rt,value);
    }

    public abstract void setProperty(final FaunusRelationType type, final Object value);

    @Override
    public <T> T removeProperty(final String key) {
        FaunusRelationType rt = getTypeManager().getRelationType(key);
        if (rt==null) return null;
        return removeProperty(rt);
    }

    @Override
    public <O> O removeProperty(RelationType type) {
        if (type.isEdgeLabel() && !(this instanceof FaunusVertex)) throw new IllegalArgumentException("Provided argument" +
                "identifies an edge label. Use edge methods to remove those: " + type);
        if (outAdjacency.isEmpty()) return null;
        FaunusRelationType rtype = (FaunusRelationType)type;
        final List<Object> removed = Lists.newArrayList();
        final Iterator<FaunusRelation> rels = outAdjacency.get(rtype).iterator();
        while (rels.hasNext()) {
            FaunusRelation r = rels.next();
            if (!r.isRemoved()) {
                if (r.isProperty()) removed.add(((FaunusProperty)r).getValue());
                else removed.add(((FaunusEdge)r).getVertex(Direction.IN));
            }
            if (r.isNew()) rels.remove();
            r.updateLifeCycle(ElementLifeCycle.Event.REMOVED);
            updateLifeCycle(ElementLifeCycle.Event.REMOVED_RELATION);
        }
        if (removed.isEmpty()) return null;
        else if (getAdjustedMultiplicity(rtype)==Multiplicity.MANY2ONE) return (O)removed.iterator().next();
        else return (O) removed;
    }

    public TitanVertex getProperty(EdgeLabel label) {
        Preconditions.checkArgument(label!=null);
        Preconditions.checkArgument(!(this instanceof FaunusVertex),"Use getEdges() to query for edges on a vertex");
        return Iterables.getOnlyElement(query().type(label).titanEdges()).getVertex(Direction.IN);
    }

    @Override
    public <T> T getProperty(PropertyKey key) {
        FaunusPropertyKey type = (FaunusPropertyKey)key;
        Iterator<TitanProperty> properties = query().type(type).properties().iterator();
        if (type.getCardinality()==Cardinality.SINGLE) {
            if (properties.hasNext()) return properties.next().getValue();
            else return (T)null;
        }
        List result = Lists.newArrayList();
        while (properties.hasNext()) result.add(properties.next().getValue());
        return (T)result;
    }

    @Override
    public <T> T getProperty(final String key) {
        FaunusRelationType rt = getTypeManager().getRelationType(key);
        if (rt==null) return null;
        if (rt.isPropertyKey()) return getProperty((FaunusPropertyKey)rt);
        else return (T)getProperty((FaunusEdgeLabel)rt);
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
        for (final TitanRelation r : query().relations()) {
            if (r.isEdge() && (this instanceof FaunusVertex)) continue;
            result.add(r.getType());
        }
        return result;
    }

    public void addAllProperties(final Iterable<FaunusRelation> properties) {
        for (final FaunusRelation p : properties) addRelation(p);
    }

    public Collection<FaunusRelation> getPropertyCollection() {
        return (Collection)Lists.newArrayList(
                (this instanceof FaunusVertex)?query().properties():query().relations());
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
        return Longs.compare(id, o.getLongId());
    }
}
