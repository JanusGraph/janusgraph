package com.thinkaurelius.titan.graphdb.types;

import com.carrotsearch.hppc.LongIntMap;
import com.thinkaurelius.titan.core.TypeGroup;
import com.thinkaurelius.titan.graphdb.relations.EdgeDirection;
import com.tinkerpop.blueprints.Direction;

public class AbstractTypeDefinition implements TypeDefinition {

    private String name;
    private TypeGroup group;
    private boolean[] isUnique;
    private boolean[] hasUniqueLock;
    private boolean[] isStatic;
    private boolean isHidden;
    private boolean isModifiable;
    private long[] primaryKey;
    private long[] signature;

    private transient LongIntMap signatureIndex = null;

    //Needed for de-serialization
    AbstractTypeDefinition() {
    }

    AbstractTypeDefinition(String name, TypeGroup group,
                                     boolean[] unique, boolean[] hasUniqueLock, boolean[] isStatic,
                                     boolean hidden, boolean modifiable,
                                     long[] primaryKey, long[] signature) {
        this.name = name;
        this.group = group;
        isUnique = unique;
        this.hasUniqueLock = hasUniqueLock;
        this.isStatic = isStatic;
        isHidden = hidden;
        isModifiable = modifiable;
        this.primaryKey = primaryKey;
        this.signature = signature;
    }

//    private LongIntMap getSignatureIndex() {
//        if (signatureIndex == null) {
//            signatureIndex = new LongIntOpenHashMap(signature.length);
//            int pos = 0;
//            for (long s : signature) {
//                signatureIndex.put(s, pos);
//                pos++;
//            }
//        }
//        return signatureIndex;
//    }
//
//    public boolean hasSignatureEdgeType(TitanType et) {
//        return getSignatureIndex().containsKey(et.getID());
//    }
//
//    public int getSignatureIndex(TitanType et) {
//        if (!hasSignatureEdgeType(et)) throw new IllegalArgumentException("The provided TitanType is not part of the signature: " + et);
//        return getSignatureIndex().get(et.getID());
//    }


    @Override
    public boolean uniqueLock(Direction direction) {
        return isUnique(direction) && hasUniqueLock[EdgeDirection.position(direction)];
    }

    @Override
    public boolean isUnique(Direction direction) {
        return isUnique[EdgeDirection.position(direction)];
    }

    @Override
    public boolean isStatic(Direction direction) {
        return isStatic[EdgeDirection.position(direction)];
    }

    @Override
    public long[] getPrimaryKey() {
        return primaryKey;
    }

    @Override
    public long[] getSignature() {
        return signature;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TypeGroup getGroup() {
        return group;
    }


    @Override
    public boolean isModifiable() {
        return isModifiable;
    }

    @Override
    public boolean isHidden() {
        return isHidden;
    }


}
