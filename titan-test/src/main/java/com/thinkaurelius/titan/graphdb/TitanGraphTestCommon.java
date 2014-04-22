package com.thinkaurelius.titan.graphdb;


import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.core.TitanLabel;
import com.thinkaurelius.titan.core.attribute.Decimal;
import com.tinkerpop.blueprints.Vertex;

public abstract class TitanGraphTestCommon extends TitanGraphBaseTest {

//    public TitanLabel makeSimpleEdgeLabel(String name) {
//        return tx.makeLabel(name).make();
//    }

//    public TitanLabel makeKeyedEdgeLabel(String name, TitanKey sort, TitanKey signature) {
//        TitanLabel relType = tx.makeLabel(name).
//                sortKey(sort).signature(signature).directed().make();
//        return relType;
//    }

//    public TitanKey makeUniqueStringPropertyKey(String name) {
//        return tx.makeKey(name).single().unique().indexed(Vertex.class).dataType(String.class).make();
//    }
//
//    public TitanKey makeStringPropertyKey(String name) {
//        return tx.makeKey(name).single().
//                indexed(Vertex.class).
//                dataType(String.class).
//                make();
//    }
//
//    public TitanKey makeUnindexedStringPropertyKey(String name) {
//        return tx.makeKey(name).single().
//                dataType(String.class).
//                make();
//    }
//
//    public TitanKey makeIntegerUIDPropertyKey(String name) {
//        return tx.makeKey(name).single().unique().indexed(Vertex.class).
//                dataType(Integer.class).
//                make();
//    }
//
//    public TitanKey makeWeightPropertyKey(String name) {
//        return tx.makeKey(name).single().
//                dataType(Decimal.class).
//                make();
//    }


}
