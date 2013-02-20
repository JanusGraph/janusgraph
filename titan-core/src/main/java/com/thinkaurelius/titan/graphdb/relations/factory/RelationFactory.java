package com.thinkaurelius.titan.graphdb.relations.factory;

import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.graphdb.internal.InternalVertex;
import com.thinkaurelius.titan.graphdb.internal.InternalRelation;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

/**
 * The logic to connect edges with vertices (i.e. add edges to the respective vertices' adjacency
 * list) remains outside of the RelationFactory
 *
 * @author matthias
 */
public interface RelationFactory {

    InternalRelation createNewProperty(TitanKey type, InternalVertex node, Object attribute);

    InternalRelation createNewRelationship(TitanLabel type, InternalVertex start, InternalVertex end);

    void setTransaction(InternalTitanTransaction tx);


}
