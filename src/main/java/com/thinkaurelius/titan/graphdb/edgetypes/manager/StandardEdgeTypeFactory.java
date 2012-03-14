package com.thinkaurelius.titan.graphdb.edgetypes.manager;

import com.thinkaurelius.titan.core.PropertyType;
import com.thinkaurelius.titan.core.RelationshipType;
import com.thinkaurelius.titan.graphdb.adjacencylist.InitialAdjListFactory;
import com.thinkaurelius.titan.graphdb.edgequery.StandardEdgeQuery;
import com.thinkaurelius.titan.graphdb.edges.factory.EdgeLoader;
import com.thinkaurelius.titan.graphdb.edgetypes.InternalEdgeType;
import com.thinkaurelius.titan.graphdb.edgetypes.PropertyTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.RelationshipTypeDefinition;
import com.thinkaurelius.titan.graphdb.edgetypes.nodes.PersistNodePropertyType;
import com.thinkaurelius.titan.graphdb.edgetypes.nodes.PersistNodeRelationshipType;
import com.thinkaurelius.titan.graphdb.edgetypes.system.SystemPropertyType;
import com.thinkaurelius.titan.graphdb.transaction.GraphTx;

public class StandardEdgeTypeFactory implements EdgeTypeFactory {

	
	public StandardEdgeTypeFactory() {
	}
	
	
	@Override
	public InternalEdgeType createExistingEdgeType(long id, 
			EdgeTypeInformation info, GraphTx tx) {
		EdgeLoader fac = tx.getEdgeFactory();
		StandardEdgeQuery loaded = null;
		InternalEdgeType edgetype = null;
		if (info.definition instanceof RelationshipTypeDefinition) {
			edgetype = new PersistNodeRelationshipType(tx,InitialAdjListFactory.BasicFactory,id);
			loaded = new StandardEdgeQuery(edgetype).includeHidden();
			fac.createExistingProperty(info.definitionEdgeID, SystemPropertyType.RelationshipTypeDefinition, edgetype, info.definition);
			edgetype.loadedEdges(loaded.withEdgeType(SystemPropertyType.RelationshipTypeDefinition));
		} else if (info.definition instanceof PropertyTypeDefinition) {
			edgetype = new PersistNodePropertyType(tx,InitialAdjListFactory.BasicFactory,id);
			loaded = new StandardEdgeQuery(edgetype).includeHidden();
			fac.createExistingProperty(info.definitionEdgeID, SystemPropertyType.PropertyTypeDefinition, edgetype, info.definition);
			edgetype.loadedEdges(loaded.withEdgeType(SystemPropertyType.PropertyTypeDefinition));
		} else throw new AssertionError("Cannot create existing edge type: Unexpected definition type: " + info.definition);

		fac.createExistingProperty(info.nameEdgeID,SystemPropertyType.EdgeTypeName, edgetype, info.definition.getName());
		edgetype.loadedEdges(loaded.withEdgeType(SystemPropertyType.EdgeTypeName));
		return edgetype;
	}

	@Override
	public InternalEdgeType createExistingPropertyType(long id, GraphTx tx) {
		PersistNodePropertyType prop = new PersistNodePropertyType(tx,InitialAdjListFactory.BasicFactory,id);
		//Load System Edges;
		prop.getDefinition(); prop.getName();
		return prop;
	}

	@Override
	public InternalEdgeType createExistingRelationshipType(long id, GraphTx tx) {
		PersistNodeRelationshipType rel = new PersistNodeRelationshipType(tx,InitialAdjListFactory.BasicFactory,id);
		//Load System Edges;
		rel.getDefinition(); rel.getName();
		return rel;
	}

	@Override
	public PropertyType createNewPropertyType(PropertyTypeDefinition def, GraphTx tx) {
		PersistNodePropertyType prop = new PersistNodePropertyType(tx,InitialAdjListFactory.BasicFactory);
		prop.createProperty(SystemPropertyType.PropertyTypeDefinition, def);
		prop.createProperty(SystemPropertyType.EdgeTypeName, def.getName());
		return prop;
	}

	@Override
	public RelationshipType createNewRelationshipType(
			RelationshipTypeDefinition def, GraphTx tx) {
		PersistNodeRelationshipType rel = new PersistNodeRelationshipType(tx,InitialAdjListFactory.BasicFactory);
		rel.createProperty(SystemPropertyType.RelationshipTypeDefinition, def);
		rel.createProperty(SystemPropertyType.EdgeTypeName, def.getName());
		return rel;
	}

}
