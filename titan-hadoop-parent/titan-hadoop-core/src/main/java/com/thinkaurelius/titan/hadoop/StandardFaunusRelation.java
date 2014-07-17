package com.thinkaurelius.titan.hadoop;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.tinkerpop.blueprints.Direction;

import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class StandardFaunusRelation extends FaunusPathElement implements FaunusRelation {

    private FaunusRelationType type;

    private static final Logger log =
            LoggerFactory.getLogger(StandardFaunusRelation.class);

    public StandardFaunusRelation(Configuration config, long id, FaunusRelationType type) {
        super(config,id);
        this.type=type;
        Preconditions.checkNotNull(this.type);
    }

    @Override
    void updateSchema(final FaunusSerializer.Schema schema) {
        super.updateSchema(schema);
        schema.add(type);
    }

    @Override
    public void setProperty(FaunusRelationType type, Object value) {
        if (type.isPropertyKey()) {
            Preconditions.checkArgument(!((FaunusPropertyKey)type).isImplicit(),"Cannot set implicit properties: %s",type);
            setRelation(new SimpleFaunusProperty((FaunusPropertyKey)type,value));
        } else {
            FaunusEdgeLabel label = (FaunusEdgeLabel)type;
            Preconditions.checkArgument(value instanceof FaunusVertex,"Vertex expected but got: %s",value);
            setRelation(new SimpleFaunusEdge(label, (FaunusVertex) value));
        }
    }

    public String getTypeName() {
        return null != type ? type.getName() : null;
    }

    @Override
    public boolean isProperty() {
        return type.isPropertyKey();
    }

    @Override
    public boolean isEdge() {
        return type.isEdgeLabel();
    }

    public int getArity() {
        return type.isPropertyKey()?1:2;
    }

    public abstract TitanVertex getVertex(int pos);

    /* ---------------------------------------------------------------
	 * Copied from AbstractTypedRelation
	 * ---------------------------------------------------------------
	 */

    @Override
    public Direction getDirection(TitanVertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (getVertex(i).equals(vertex)) return EdgeDirection.fromPosition(i);
        }
        throw new IllegalArgumentException("Relation is not incident on vertex");
    }

    @Override
    public boolean isIncidentOn(TitanVertex vertex) {
        for (int i=0;i<getArity();i++) {
            if (getVertex(i).equals(vertex)) return true;
        }
        return false;
    }

    @Override
    public boolean isHidden() {
        return type.isHiddenType();
    }

    @Override
    public boolean isLoop() {
        return getArity()==2 && getVertex(0).equals(getVertex(1));
    }

    @Override
    public FaunusRelationType getType() {
        return type;
    }

    @Override
    public Object getId() {
        if (!hasId()) return null;
        long[] ids = new long[isProperty()?3:4];
        ids[0]=getLongId();
        ids[2]=type.getLongId();
        if (isProperty()) {
            ids[1]=((StandardFaunusProperty)this).getVertex().getLongId();
        } else {
            StandardFaunusEdge edge = (StandardFaunusEdge)this;
            ids[1]=edge.getVertex(Direction.OUT).getLongId();
            ids[3]=edge.getVertex(Direction.IN).getLongId();
        }
        for (int i = 0; i < ids.length; i++) {
            if (ids[i]<=0) return null;
        }
        return RelationIdentifier.get(ids);
    }

    /* ---------------------------------------------------------------
	 * Map onto existing methods
	 * ---------------------------------------------------------------
	 */

    public Iterable<RelationType> getPropertyKeysDirect() {
        return super.getPropertyKeysDirect();
    }


    protected void setType(FaunusRelationType t) {
        type = t;
        Preconditions.checkNotNull(type);
    }

}
