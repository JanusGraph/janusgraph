package com.thinkaurelius.titan.graphdb.types.manager;

import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.core.InvalidElementException;
import com.thinkaurelius.titan.graphdb.database.InternalTitanGraph;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.system.SystemKey;
import com.thinkaurelius.titan.graphdb.idmanagement.IDInspector;
import com.thinkaurelius.titan.graphdb.transaction.InternalTitanTransaction;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.thinkaurelius.titan.graphdb.types.manager.TypeManagerUtil.convertSignature;

public class SimpleTypeManager implements TypeManager {

    private final InternalTitanGraph graphdb;
    private final TypeFactory factory;

    private final ReadWriteLock mapLock;
    private final Lock mapReadLock;
    private final Lock mapWriteLock;

    private final Map<Long, TypeInformation> idIndex;
    private final Map<String, Long> nameIndex;

    public SimpleTypeManager(InternalTitanGraph graphdb) {
        this.graphdb = graphdb;
        idIndex = new HashMap<Long, TypeInformation>();
        nameIndex = new HashMap<String, Long>();

        factory = new StandardTypeFactory();

        mapLock = new ReentrantReadWriteLock();
        mapReadLock = mapLock.readLock();
        mapWriteLock = mapLock.writeLock();
    }

    public void close() {
        idIndex.clear();
        nameIndex.clear();
    }

    @Override
    public boolean containsType(long id, InternalTitanTransaction tx) {
        boolean contains = false;
        mapReadLock.lock();
        try {
            contains = idIndex.containsKey(Long.valueOf(id));
        } finally {
            mapReadLock.unlock();
        }
        return contains ? true : graphdb.containsVertexID(id, tx);
    }

    @Override
    public boolean containsType(String name, InternalTitanTransaction tx) {
        boolean contains = false;
        mapReadLock.lock();
        try {
            contains = nameIndex.containsKey(name);
        } finally {
            mapReadLock.unlock();
        }
        return contains ? true : graphdb.indexRetrieval(name, SystemKey.TypeName, tx).length > 0;
    }

    @Override
    public void committed(InternalTitanType type) {
        Long id = type.getID();
        mapWriteLock.lock();
        try {
            if (nameIndex.containsKey(type.getName()))
                throw new InvalidElementException("TitanRelation Type with name does already exist: " + type.getName()
                        + " | " + type.isEdgeLabel(), type);
            nameIndex.put(type.getName(), id);
            // Determine system edge ids
            long nameEdgeID = QueryUtil.queryHiddenFunctionalProperty(type, SystemKey.TypeName).getID();
            long defEdgeID = -1;
            if (type.isPropertyKey()) {
                defEdgeID = QueryUtil.queryHiddenFunctionalProperty(type, SystemKey.PropertyTypeDefinition).getID();
            } else {
                assert type.isEdgeLabel();
                defEdgeID = QueryUtil.queryHiddenFunctionalProperty(type, SystemKey.RelationshipTypeDefinition).getID();
            }
            idIndex.put(id, new TypeInformation(type.getDefinition(), defEdgeID, nameEdgeID));
        } finally {
            mapWriteLock.unlock();
        }
    }

    private void checkUniqueName(String name) {
        mapReadLock.lock();
        try {
            if (nameIndex.containsKey(name))
                throw new IllegalArgumentException("TitanRelation Type with name does already exist: " + name);
        } finally {
            mapReadLock.unlock();
        }
    }

    @Override
    public TitanKey createPropertyKey(InternalTitanTransaction tx, String name, TypeCategory category,
            Directionality directionality, TypeVisibility visibility, FunctionalType isfunctional, TitanType[] keysig,
            TitanType[] compactsig, TypeGroup group, boolean isKey, boolean hasIndex, Class<?> objectType) {
        checkUniqueName(name);
        StandardPropertyKey pt = new StandardPropertyKey(name, category, directionality, visibility, isfunctional,
                convertSignature(keysig), convertSignature(compactsig), group, isKey, hasIndex, objectType);
        return factory.createNewPropertyKey(pt, tx);
    }

    @Override
    public TitanLabel createEdgeLabel(InternalTitanTransaction tx, String name, TypeCategory category,
            Directionality directionality, TypeVisibility visibility, FunctionalType isfunctional, TitanType[] keysig,
            TitanType[] compactsig, TypeGroup group) {
        checkUniqueName(name);
        StandardEdgeLabel rt = new StandardEdgeLabel(name, category, directionality, visibility, isfunctional,
                convertSignature(keysig), convertSignature(compactsig), group);
        return factory.createNewEdgeLabel(rt, tx);
    }

    @Override
    public InternalTitanType getType(long id, InternalTitanTransaction tx) {
        TypeInformation info = null;
        mapReadLock.lock();
        try {
            info = idIndex.get(Long.valueOf(id));
        } finally {
            mapReadLock.unlock();
        }
        if (info == null) {
            if (!tx.containsVertex(id))
                throw new IllegalArgumentException("TitanType is unknown: " + id);
            IDInspector idspec = graphdb.getIDInspector();
            assert idspec.isEdgeTypeID(id);
            InternalTitanType et = null;
            if (idspec.isPropertyTypeID(id)) {
                et = factory.createExistingPropertyKey(id, tx);
            } else if (idspec.isRelationshipTypeID(id)) {
                et = factory.createExistingEdgeLabel(id, tx);
            } else
                throw new AssertionError("Unexpected type id: " + id);
            mapWriteLock.lock();
            try {
                if (idIndex.containsKey(Long.valueOf(id)))
                    et = getType(id, tx);
                else
                    committed(et);
            } finally {
                mapWriteLock.unlock();
            }
            return et;
        } else {
            return factory.createExistingType(id, info, tx);
        }
    }

    @Override
    public InternalTitanType getType(String name, InternalTitanTransaction tx) {
        Long id = null;
        mapReadLock.lock();
        try {
            id = nameIndex.get(name);
        } finally {
            mapReadLock.unlock();
        }
        if (id == null) {
            long[] ids = graphdb.indexRetrieval(name, SystemKey.TypeName, tx);
            if (ids.length == 0)
                return null;
            else {
                assert ids.length == 1;
                id = ids[0];
            }
        }
        return getType(id, tx);
    }

    @Override
    public TypeMaker getTypeMaker(InternalTitanTransaction tx) {
        return new StandardTypeMaker(tx, this);
    }

}
