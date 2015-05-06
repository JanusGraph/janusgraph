package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.Cardinality;

import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;

import com.thinkaurelius.titan.graphdb.internal.*;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.commons.lang.StringUtils;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.TemporalUnit;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImplicitKey extends EmptyRelationType implements SystemRelationType, PropertyKey {

    public static final ImplicitKey ID = new ImplicitKey(1001, T.id.getAccessor() ,Object.class);

    public static final ImplicitKey TITANID = new ImplicitKey(1002,Token.makeSystemName("nid"),Long.class);

    public static final ImplicitKey LABEL = new ImplicitKey(11, T.label.getAccessor() ,String.class);

    public static final ImplicitKey KEY = new ImplicitKey(12, T.key.getAccessor(), String.class);

    public static final ImplicitKey VALUE = new ImplicitKey(13, T.value.getAccessor(), Object.class);

    public static final ImplicitKey ADJACENT_ID = new ImplicitKey(1003,Token.makeSystemName("adjacent"),Long.class);

    //######### IMPLICIT KEYS WITH ID ############

    public static final ImplicitKey TIMESTAMP = new ImplicitKey(5,Token.makeSystemName("timestamp"),Instant.class);

    public static final ImplicitKey VISIBILITY = new ImplicitKey(6,Token.makeSystemName("visibility"),String.class);

    public static final ImplicitKey TTL = new ImplicitKey(7,Token.makeSystemName("ttl"), Duration.class);


    public static final Map<EntryMetaData,ImplicitKey> MetaData2ImplicitKey = ImmutableMap.of(
            EntryMetaData.TIMESTAMP,TIMESTAMP,
            EntryMetaData.TTL,TTL,
            EntryMetaData.VISIBILITY,VISIBILITY);

    private final Class<?> datatype;
    private final String name;
    private final long id;

    private ImplicitKey(final long id, final String name, final Class<?> datatype) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name) && datatype!=null && id>0);
        assert Token.isSystemName(name);
        this.datatype=datatype;
        this.name=name;
        this.id= BaseRelationType.getSystemTypeId(id, TitanSchemaCategory.PROPERTYKEY);
    }


    public<O> O computeProperty(InternalElement e) {
        if (this==ID) {
            return (O)e.id();
        } else if (this==TITANID) {
            return (O)Long.valueOf(e.longId());
        } else if (this==LABEL) {
            return (O)e.label();
        } else if (this==KEY) {
            if (e instanceof TitanProperty) return (O)((TitanProperty)e).key();
            else return null;
        } else if (this==VALUE) {
            if (e instanceof TitanProperty) return (O)((TitanProperty)e).value();
            else return null;
        } else if (this==TIMESTAMP || this==VISIBILITY) {
            if (e instanceof InternalRelation) {
                InternalRelation r = (InternalRelation) e;
                if (this==VISIBILITY) {
                    return r.getValueDirect(this);
                } else {
                    assert this == TIMESTAMP;
                    Long time = r.getValueDirect(this);
                    if (time==null) return null; //there is no timestamp
                    return (O) r.tx().getConfiguration().getTimestampProvider().getTime(time);
                }
            } else {
                return null;
            }
        } else if (this == TTL) {
            int ttl;
            if (e instanceof InternalRelation) {
                ttl = ((InternalRelationType)((InternalRelation) e).getType()).getTTL();
            } else if (e instanceof InternalVertex) {
                ttl = ((InternalVertexLabel)((InternalVertex) e).vertexLabel()).getTTL();
            } else {
                ttl = 0;
            }
            return (O) Duration.ofSeconds(ttl);
        } else throw new AssertionError("Implicit key property is undefined: " + this.name());
    }

    @Override
    public Class<?> dataType() {
        return datatype;
    }

    @Override
    public Cardinality cardinality() {
        return Cardinality.SINGLE;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean isPropertyKey() {
        return true;
    }

    @Override
    public boolean isEdgeLabel() {
        return false;
    }

    @Override
    public boolean isInvisibleType() {
        return false;
    }

    @Override
    public Multiplicity multiplicity() {
        return Multiplicity.convert(cardinality());
    }

    @Override
    public ConsistencyModifier getConsistencyModifier() {
        return ConsistencyModifier.DEFAULT;
    }

    @Override
    public boolean isUnidirected(Direction dir) {
        return dir==Direction.OUT;
    }

    @Override
    public long longId() {
        return id;
    }

    @Override
    public boolean hasId() {
        return id>0;
    }

    @Override
    public void setId(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }

    @Override
    public String toString() {
        return name;
    }

}
