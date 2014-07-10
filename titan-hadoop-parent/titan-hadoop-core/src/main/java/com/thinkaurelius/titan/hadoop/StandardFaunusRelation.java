package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.thinkaurelius.titan.graphdb.relations.RelationIdentifier;
import com.tinkerpop.blueprints.Direction;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public abstract class StandardFaunusRelation extends FaunusPathElement implements FaunusRelation {

    protected FaunusRelationType type;

    public StandardFaunusRelation(Configuration config, long id, FaunusRelationType type) {
        super(config,id);
        this.type=type;
    }

    @Override
    public void setProperty(EdgeLabel label, TitanVertex vertex) {
        ...
    }

    @Override
    public TitanVertex getProperty(EdgeLabel label) {

    }

    public String getTypeName() {
        return type.getName();
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
    public RelationIdentifier getId() {
        return RelationIdentifier.get(this);
    }

    /* ---------------------------------------------------------------
	 * Map onto existing methods
	 * ---------------------------------------------------------------
	 */

    public Iterable<RelationType> getPropertyKeysDirect() {
        return super.getPropertyKeysDirect();
    }



}
