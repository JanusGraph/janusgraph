package com.thinkaurelius.titan.graphdb.relations;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.AbstractElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.InternalRelationType;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.system.ImplicitKey;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.stream.Stream;

public abstract class AbstractTypedRelation extends AbstractElement implements InternalRelation {

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
            throw InvalidElementException.removedException(this);

        return next;
    }

    @Override
    public final StandardTitanTx tx() {
        return getVertex(0).tx();
    }

    /**
     * Cannot currently throw exception when removed since internal logic relies on access to the edge
     * beyond its removal. TODO: re-concile with access validation logic
     */
    protected final void verifyAccess() {
        return;
//        if (isRemoved()) {
//            throw InvalidElementException.removedException(this);
//        }
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
        verifyAccess();

        PropertyKey pkey = tx().getOrCreatePropertyKey(key);
        Object normalizedValue = tx().verifyAttribute(pkey,value);
        it().setPropertyDirect(pkey,normalizedValue);
        return new SimpleTitanProperty<V>(this,pkey,value);
    }

    @Override
    public <O> O valueOrNull(PropertyKey key) {
        verifyAccess();
        if (key instanceof ImplicitKey) return ((ImplicitKey)key).computeProperty(this);
        return it().getValueDirect(key);
    }

    @Override
    public <O> O value(String key) {
        verifyAccess();
        O val = valueInternal(tx().getPropertyKey(key));
        if (val==null) throw Property.Exceptions.propertyDoesNotExist(this,key);
        return val;
    }

    private <O> O valueInternal(PropertyKey type) {
        if (type==null) {
            return null;
        }
        return valueOrNull(type);
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... keyNames) {
        verifyAccess();

        Stream<PropertyKey> keys;

        if (keyNames==null || keyNames.length==0) {
            keys = IteratorUtils.stream(it().getPropertyKeysDirect().iterator());
        } else {
            keys = Stream.of(keyNames)
                    .map(s -> tx().getPropertyKey(s)).filter(rt -> rt != null && getValueDirect(rt)!=null);
        }
        return keys.map( rt -> (Property<V>)new SimpleTitanProperty<V>(this,rt,valueInternal(rt))).iterator();
    }

    /* ---------------------------------------------------------------
	 * Blueprints Iterators
	 * ---------------------------------------------------------------
	 */

//    @Override
//    public Iterator<Vertex> vertexIterator(Direction direction) {
//        verifyAccess();
//
//        List<Vertex> vertices;
//        if (direction==Direction.BOTH) {
//            vertices = ImmutableList.of((Vertex)getVertex(0),getVertex(1));
//        } else {
//            vertices = ImmutableList.of((Vertex)getVertex(EdgeDirection.position(direction)));
//        }
//        return vertices.iterator();
//    }
//
//    @Override
//    public <V> Iterator<Property<V>> propertyIterator(String... keyNames) {
//        verifyAccess();
//
//        Stream<RelationType> keys;
//
//        if (keyNames==null || keyNames.length==0) {
//            keys = IteratorUtils.stream(it().getPropertyKeysDirect());
//        } else {
//            keys = Stream.of(keyNames)
//                    .map(s -> tx().getRelationType(s)).filter(rt -> rt != null && getValueDirect(rt)!=null);
//        }
//        return keys.map( rt -> (Property<V>)new SimpleTitanProperty<V>(this,rt,valueInternal(rt))).iterator();
//    }


}
