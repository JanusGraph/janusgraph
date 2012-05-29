package com.thinkaurelius.titan.graphdb.types.system;

import com.google.common.collect.ImmutableList;
import com.thinkaurelius.titan.graphdb.types.Directionality;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;

public class SystemLabel extends SystemType implements EdgeLabelDefinition, TitanLabel {
	
	//public static final SystemLabel OtherNode = new SystemLabel("OtherNode",1);

	public static final SystemLabel TAIL = new SystemLabel("Tail",2);

	public static final SystemLabel END = new SystemLabel("Head",3);

	public static final SystemLabel TYPE = new SystemLabel("TitanType",4);

	public static final Iterable<SystemLabel> values() {
		return ImmutableList.of(TAIL, END, TYPE);
	}
	
	
	private SystemLabel(String name, int id) {
		super(name, IDManager.getSystemEdgeLabelID(id));
	}

	@Override
	public boolean isFunctional() {
		return true;
	}

    @Override
    public boolean isFunctionalLocking() {
        return false;
    }

	@Override
	public Directionality getDirectionality() {
		return Directionality.Unidirected;
	}
	
	@Override
	public final boolean isPropertyKey() {
		return false;
	}
	
	@Override
	public final boolean isEdgeLabel() {
		return true;
	}


    @Override
    public boolean isDirected() {
        return true;
    }

    @Override
    public boolean isUndirected() {
        return false;
    }

    @Override
    public boolean isUnidirected() {
        return false;
    }
}
