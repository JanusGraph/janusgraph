package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.attribute.Duration;
import com.thinkaurelius.titan.core.attribute.Timestamp;
import com.thinkaurelius.titan.core.Cardinality;
import com.thinkaurelius.titan.core.schema.ConsistencyModifier;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.diskstorage.EntryMetaData;
import com.thinkaurelius.titan.diskstorage.util.time.StandardDuration;
import com.thinkaurelius.titan.diskstorage.util.time.StandardTimestamp;
import com.thinkaurelius.titan.graphdb.internal.InternalElement;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.tinkerpop.blueprints.Direction;
import org.apache.commons.lang.StringUtils;
import static com.thinkaurelius.titan.graphdb.internal.Token.*;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class ImplicitKey extends EmptyRelationType implements SystemRelationType, PropertyKey {

    public static final ImplicitKey ID = new ImplicitKey(1001,"id",Object.class);

    public static final ImplicitKey TITANID = new ImplicitKey(1002,SPECIAL_TYPE_CHAR+"titanid",Long.class);

    public static final ImplicitKey LABEL = new ImplicitKey(11,"label",String.class);

//    public static final ImplicitKey KEY = new ImplicitKey("key",Long.class);

    public static final ImplicitKey ADJACENT_ID = new ImplicitKey(1003,SPECIAL_TYPE_CHAR+"adjacent",Long.class);

    //######### IMPLICIT KEYS WITH ID ############

    public static final ImplicitKey TIMESTAMP = new ImplicitKey(5,SPECIAL_TYPE_CHAR+"timestamp",Timestamp.class);

    public static final ImplicitKey VISIBILITY = new ImplicitKey(6,SPECIAL_TYPE_CHAR+"visibility",String.class);

    public static final ImplicitKey TTL = new ImplicitKey(7,SPECIAL_TYPE_CHAR+"ttl",Duration.class);


    public static final Map<EntryMetaData,ImplicitKey> MetaData2ImplicitKey = ImmutableMap.of(
            EntryMetaData.TIMESTAMP,TIMESTAMP,
            EntryMetaData.TTL,TTL,
            EntryMetaData.VISIBILITY,VISIBILITY);

    private final Class<?> datatype;
    private final String name;
    private final long id;

    private ImplicitKey(final long id, final String name, final Class<?> datatype) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name) && datatype!=null && id>0);
        this.datatype=datatype;
        this.name=name;
        this.id= BaseRelationType.getSystemTypeId(id, TitanSchemaCategory.PROPERTYKEY);
    }


    public<O> O computeProperty(InternalElement e) {
        if (this==ID) {
            return (O)e.getId();
        } else if (this==TITANID) {
            return (O)Long.valueOf(e.getID());
        } else if (this==LABEL) {
            if (e instanceof TitanEdge) {
                return (O)((TitanEdge) e).getLabel();
            } else if (e instanceof TitanVertex) {
                return (O)((TitanVertex)e).getLabel();
            } else if (e instanceof TitanProperty) {
                return (O)((TitanProperty)e).getPropertyKey().getName();
            } else {
                return null;
            }
        } else if (this==TIMESTAMP || this==VISIBILITY || this==TTL) {
            if (e instanceof InternalRelation) {
                InternalRelation r = (InternalRelation) e;
                if (this==VISIBILITY) {
                    return r.getPropertyDirect(this);
                } else {
                    assert this==TIMESTAMP || this==TTL;
                    Long time = r.getPropertyDirect(this);
                    if (time==null) return null; //there is no timestamp or ttl
                    TimeUnit unit = r.tx().getConfiguration().getTimestampProvider().getUnit();
                    if (this==TIMESTAMP) return (O)new StandardTimestamp(time,unit);
                    else return (O)new StandardDuration(time,unit);
                }
            } else {
                return null;
            }
        } else throw new AssertionError("Implicit key property is undefined: " + this.getName());
    }


    @Override
    public Class<?> getDataType() {
        return datatype;
    }

    @Override
    public Cardinality getCardinality() {
        return Cardinality.SINGLE;
    }

    @Override
    public String getName() {
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
    public boolean isHiddenType() {
        return false;
    }

    @Override
    public Multiplicity getMultiplicity() {
        return Multiplicity.convert(getCardinality());
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
    public long getID() {
        return id;
    }

    @Override
    public boolean hasId() {
        return id>0;
    }

    @Override
    public void setID(long id) {
        throw new IllegalStateException("SystemType has already been assigned an id");
    }

    @Override
    public String toString() {
        return name;
    }

}
