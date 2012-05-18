package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.AtomicTitanQuery;
import com.thinkaurelius.titan.graphdb.edges.factory.RelationLoader;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalTitanType;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.edgetypes.nodes.PersistNodeTitanKey;
import com.thinkaurelius.titan.graphdb.edgetypes.nodes.PersistNodeTitanLabel;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class StandardEdgeTypeFactory implements EdgeTypeFactory {

	
	public StandardEdgeTypeFactory() {
	}
	
	
	@Override
	public InternalTitanType createExistingType(long id, EdgeTypeInformation info, InternalTitanTransaction tx) {
		RelationLoader fac = tx.getRelationFactory();
		AtomicTitanQuery loaded = null;
		InternalTitanType edgetype = null;
		if (info.definition instanceof RelationshipTypeDefinition) {
			edgetype = new PersistNodeTitanLabel(tx,InitialAdjListFactory.BasicFactory,id);
			loaded = new AtomicTitanQuery(edgetype).includeHidden();
			fac.createExistingProperty(info.definitionEdgeID, SystemKey.RelationshipTypeDefinition, edgetype, info.definition);
			edgetype.loadedEdges(loaded.type(SystemKey.RelationshipTypeDefinition));
		} else if (info.definition instanceof PropertyTypeDefinition) {
			edgetype = new PersistNodeTitanKey(tx,InitialAdjListFactory.BasicFactory,id);
			loaded = new AtomicTitanQuery(edgetype).includeHidden();
			fac.createExistingProperty(info.definitionEdgeID, SystemKey.PropertyTypeDefinition, edgetype, info.definition);
			edgetype.loadedEdges(loaded.type(SystemKey.PropertyTypeDefinition));
		} else throw new AssertionError("Cannot create existing edge type: Unexpected definition type: " + info.definition);

		fac.createExistingProperty(info.nameEdgeID, SystemKey.TypeName, edgetype, info.definition.getName());
		edgetype.loadedEdges(loaded.type(SystemKey.TypeName));
		return edgetype;
	}

	@Override
	public InternalTitanType createExistingPropertyKey(long id, InternalTitanTransaction tx) {
		PersistNodeTitanKey prop = new PersistNodeTitanKey(tx,InitialAdjListFactory.BasicFactory,id);
		//Load System Edges;
		prop.getDefinition(); prop.getName();
		return prop;
	}

	@Override
	public InternalTitanType createExistingEdgeLabel(long id, InternalTitanTransaction tx) {
		PersistNodeTitanLabel rel = new PersistNodeTitanLabel(tx,InitialAdjListFactory.BasicFactory,id);
		//Load System Edges;
		rel.getDefinition(); rel.getName();
		return rel;
	}

	@Override
	public TitanKey createNewPropertyKey(PropertyTypeDefinition def, InternalTitanTransaction tx) {
		PersistNodeTitanKey prop = new PersistNodeTitanKey(tx,InitialAdjListFactory.BasicFactory);
		prop.addProperty(SystemKey.PropertyTypeDefinition, def);
        tx.registerNewEntity(prop);
		prop.addProperty(SystemKey.TypeName, def.getName());
        prop.addProperty(SystemKey.TypeClass, TitanTypeClass.KEY);
		return prop;
	}

	@Override
	public TitanLabel createNewEdgeLabel(
            RelationshipTypeDefinition def, InternalTitanTransaction tx) {
		PersistNodeTitanLabel rel = new PersistNodeTitanLabel(tx,InitialAdjListFactory.BasicFactory);
		rel.addProperty(SystemKey.RelationshipTypeDefinition, def);
        tx.registerNewEntity(rel);
		rel.addProperty(SystemKey.TypeName, def.getName());
        rel.addProperty(SystemKey.TypeClass, TitanTypeClass.LABEL);
		return rel;
	}

}
