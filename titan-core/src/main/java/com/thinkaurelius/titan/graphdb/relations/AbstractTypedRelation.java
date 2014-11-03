package com.thinkaurelius.titan.graphdb.relations;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import com.tinkerpop.gremlin.structure.*;
import com.tinkerpop.gremlin.util.StreamFactory;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

public abstract class AbstractTypedRelation extends AbstractElement implements InternalRelation, Edge.Iterators, VertexProperty.Iterators {

    protected final InternalRelationType type;

    public AbstractTypedRelation(final long id, final RelationType type) {
        super(id);
        assert type != null && type instanceof InternalRelationType;
        this.type = (InternalRelationType) type;
    }

    @Override
    public InternalRelation it() {
        InternalVertex v = getVertex(0);
        if (v == v.it())
            return this;

        InternalRelation next = (InternalRelation) RelationIdentifier.get(this).findRelation(tx());
        if (next == null)
            throw new InvalidElementException("Relation has been removed", this);

        return next;
    }

    @Override
    public final StandardTitanTx tx() {
        return getVertex(0).tx();
    }

	/* ---------------------------------------------------------------
	 * Immutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public Direction direction(Vertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (it().getVertex(i).equals(vertex)) return EdgeDirection.fromPosition(i);
        }
        throw new IllegalArgumentException("Relation is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(Vertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (it().getVertex(i).equals(vertex)) return true;
        }
        return false;
    }

    @Override
    public boolean isInvisible() {
        return type.isInvisibleType();
    }

    public boolean isHidden() {
        return Graph.Key.isHidden(type.name());
    }

    @Override
    public boolean isLoop() {
        return getArity()==2 && getVertex(0).equals(getVertex(1));
    }

    @Override
    public RelationType getType() {
        return type;
    }

    @Override
    public RelationIdentifier id() {
        return RelationIdentifier.get(this);
    }

    /* ---------------------------------------------------------------
	 * Mutable Aspects of Relation
	 * ---------------------------------------------------------------
	 */

    @Override
    public <V> Property<V> property(final String key, final V value) {
        Preconditions.checkArgument(!it().isRemoved(),"Cannot modified removed relation");

        RelationType type = tx().getRelationType(key);
        if (type==null) {
            if (value instanceof TitanVertex) type = tx().getOrCreateEdgeLabel(key);
            type = tx().getOrCreatePropertyKey(key);
        }
        assert type!=null;

        if (type instanceof PropertyKey) {
            it().setPropertyDirect(type,tx().verifyAttribute((PropertyKey)type,value));
        } else {
            assert type.isEdgeLabel();
            Preconditions.checkArgument(((EdgeLabel)type).isUnidirected(),"Label must be unidirected");
            Preconditions.checkArgument(value!=null && value instanceof TitanVertex,"Value must be a vertex");
            it().setPropertyDirect(type,value);
        }
        return new SimpleTitanProperty<V>(this,type,value);
    }

    @Override
    public <O> O valueOrNull(RelationType key) {
        if (key instanceof ImplicitKey) return ((ImplicitKey)key).computeProperty(this);
        return it().getValueDirect(key);
    }

    @Override
    public <O> O value(String key) {
        O val = valueInternal(tx().getRelationType(key));
        if (val==null) throw Property.Exceptions.propertyDoesNotExist(key);
        return val;
    }

    private <O> O valueInternal(RelationType type) {
        if (type==null) {
            return null;
        } else if (type.isPropertyKey()) {
            return valueOrNull((PropertyKey) type);
        } else {
            assert type.isEdgeLabel();
            Object val = it().getValueDirect(type);
            if (val==null) return null;
            else if (val instanceof Number) return (O)tx().getInternalVertex(((Number) val).longValue());
            else if (val instanceof TitanVertex) return (O)val;
            else throw new IllegalStateException("Invalid object found instead of vertex: " + val);
        }
    }

    /* ---------------------------------------------------------------
	 * Blueprints Iterators
	 * ---------------------------------------------------------------
	 */

    @Override
    public Iterator<Vertex> vertexIterator(Direction direction) {
        List<Vertex> vertices;
        if (direction==Direction.BOTH) {
            vertices = ImmutableList.of((Vertex)getVertex(0),getVertex(1));
        } else {
            vertices = ImmutableList.of((Vertex)getVertex(EdgeDirection.position(direction)));
        }
        return vertices.iterator();
    }

    public <V> Iterator<Property<V>> propertyIterator(boolean hidden, String... keyNames) {
        Stream<RelationType> keys;

        if (keyNames==null || keyNames.length==0) {
            keys = StreamFactory.stream(it().getPropertyKeysDirect()).filter( rt -> hidden ^ !Graph.Key.isHidden(rt.name()));
        } else {
            if (hidden) keyNames = QueryUtil.hideKeys(keyNames);
            keys = Stream.of(keyNames)
                    .map(s -> tx().getRelationType(s)).filter(rt -> rt != null && getValueDirect(rt)!=null);
        }
        return keys.map( rt -> (Property<V>)new SimpleTitanProperty<V>(this,rt,valueInternal(rt))).iterator();
    }

    @Override
    public <V> Iterator<Property<V>> propertyIterator(String... strings) {
        return propertyIterator(false,strings);
    }

    @Override
    public <V> Iterator<Property<V>> hiddenPropertyIterator(String... strings) {
        return propertyIterator(true,strings);
    }





}
