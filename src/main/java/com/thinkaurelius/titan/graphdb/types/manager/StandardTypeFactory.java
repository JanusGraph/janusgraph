package com.thinkaurelius.titan.graphdb.types.manager;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.adjacencylist.StandardAdjListFactory;
import com.thinkaurelius.titan.graphdb.database.StandardVertexRelationLoader;
import com.thinkaurelius.titan.graphdb.database.VertexRelationLoader;
import com.thinkaurelius.titan.graphdb.query.SimpleAtomicQuery;
import com.thinkaurelius.titan.graphdb.types.InternalTitanType;
import com.thinkaurelius.titan.graphdb.types.PropertyKeyDefinition;
import com.thinkaurelius.titan.graphdb.types.EdgeLabelDefinition;
import com.thinkaurelius.titan.graphdb.types.TitanTypeClass;
import com.thinkaurelius.titan.graphdb.types.vertices.PersistVertexTitanKey;
import com.thinkaurelius.titan.graphdb.types.vertices.PersistVertexTitanLabel;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

public class StandardTypeFactory implements TypeFactory {

	
	public StandardTypeFactory() {
	}
	
	
	@Override
	public InternalTitanType createExistingType(long id, TypeInformation info, InternalTitanTransaction tx) {
		SimpleAtomicQuery loaded = null;
		InternalTitanType edgetype = null;
        VertexRelationLoader loader = null;
		if (info.definition instanceof EdgeLabelDefinition) {
			edgetype = new PersistVertexTitanLabel(tx,StandardAdjListFactory.INSTANCE,id);
            loader = new StandardVertexRelationLoader(edgetype);
			loaded = new SimpleAtomicQuery(edgetype).includeHidden();
			loader.loadProperty(info.definitionEdgeID, SystemKey.RelationshipTypeDefinition, info.definition);
			edgetype.loadedEdges(loaded.type(SystemKey.RelationshipTypeDefinition));
		} else if (info.definition instanceof PropertyKeyDefinition) {
			edgetype = new PersistVertexTitanKey(tx,StandardAdjListFactory.INSTANCE,id);
            loader = new StandardVertexRelationLoader(edgetype);
			loaded = new SimpleAtomicQuery(edgetype).includeHidden();
			loader.loadProperty(info.definitionEdgeID, SystemKey.PropertyTypeDefinition, info.definition);
			edgetype.loadedEdges(loaded.type(SystemKey.PropertyTypeDefinition));
		} else throw new AssertionError("Cannot create existing edge type: Unexpected definition type: " + info.definition);
        loader.finalizeRelation();
		loader.loadProperty(info.nameEdgeID, SystemKey.TypeName, info.definition.getName());
        loader.finalizeRelation();
		edgetype.loadedEdges(loaded.type(SystemKey.TypeName));
		return edgetype;
	}

	@Override
	public InternalTitanType createExistingPropertyKey(long id, InternalTitanTransaction tx) {
		PersistVertexTitanKey prop = new PersistVertexTitanKey(tx, StandardAdjListFactory.INSTANCE,id);
		//Load System Edges;
		prop.getDefinition(); prop.getName();
		return prop;
	}

	@Override
	public InternalTitanType createExistingEdgeLabel(long id, InternalTitanTransaction tx) {
		PersistVertexTitanLabel rel = new PersistVertexTitanLabel(tx,StandardAdjListFactory.INSTANCE,id);
		//Load System Edges;
		rel.getDefinition(); rel.getName();
		return rel;
	}

	@Override
	public TitanKey createNewPropertyKey(PropertyKeyDefinition def, InternalTitanTransaction tx) {
		PersistVertexTitanKey prop = new PersistVertexTitanKey(tx,StandardAdjListFactory.INSTANCE);
		prop.addProperty(SystemKey.PropertyTypeDefinition, def);
        tx.registerNewEntity(prop);
		prop.addProperty(SystemKey.TypeName, def.getName());
        prop.addProperty(SystemKey.TypeClass, TitanTypeClass.KEY);
		return prop;
	}

	@Override
	public TitanLabel createNewEdgeLabel(
            EdgeLabelDefinition def, InternalTitanTransaction tx) {
		PersistVertexTitanLabel rel = new PersistVertexTitanLabel(tx,StandardAdjListFactory.INSTANCE);
		rel.addProperty(SystemKey.RelationshipTypeDefinition, def);
        tx.registerNewEntity(rel);
		rel.addProperty(SystemKey.TypeName, def.getName());
        rel.addProperty(SystemKey.TypeClass, TitanTypeClass.LABEL);
		return rel;
	}

}
