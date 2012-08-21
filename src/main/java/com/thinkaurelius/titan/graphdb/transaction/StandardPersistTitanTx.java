package com.thinkaurelius.titan.graphdb.transaction;

import cern.colt.list.AbstractLongList;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.thinkaurelius.titan.core.GraphStorageException;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.core.TitanKey;
import com.thinkaurelius.titan.diskstorage.TransactionHandle;
import com.thinkaurelius.titan.graphdb.database.InternalTitanGraph;
import com.thinkaurelius.titan.graphdb.query.AtomicQuery;
import com.thinkaurelius.titan.graphdb.relations.AttributeUtil;
import com.thinkaurelius.titan.graphdb.relations.InternalRelation;
import com.thinkaurelius.titan.graphdb.relations.factory.StandardPersistedRelationFactory;
import com.thinkaurelius.titan.graphdb.types.manager.TypeManager;
import com.thinkaurelius.titan.graphdb.vertices.InternalTitanVertex;
import com.thinkaurelius.titan.graphdb.vertices.factory.StandardVertexFactories;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class StandardPersistTitanTx extends AbstractTitanTx {

	private static final Logger log = LoggerFactory.getLogger(StandardPersistTitanTx.class);

	private final TransactionHandle txHandle;
		
	private Set<InternalRelation> deletedEdges;
	private List<InternalRelation> addedEdges;


	public StandardPersistTitanTx(InternalTitanGraph g, TypeManager etManage, TransactionConfig config,
                                  TransactionHandle tx) {
		super(g, StandardVertexFactories.DefaultPersisted,new StandardPersistedRelationFactory(),
				etManage,config);
		Preconditions.checkNotNull(g);
		txHandle = tx;

		if (config.isReadOnly()) {
			deletedEdges = ImmutableSet.of();
			addedEdges = ImmutableList.of();
		} else {
			deletedEdges = Collections.newSetFromMap(new ConcurrentHashMap<InternalRelation,Boolean>(10,0.75f,1));
			addedEdges = Collections.synchronizedList(new ArrayList<InternalRelation>());
		}
	}


	/* ---------------------------------------------------------------
	 * TitanVertex and TitanRelation creation
	 * ---------------------------------------------------------------
	 */

	@Override
	public boolean isDeletedRelation(InternalRelation relation) {
		if (relation.isRemoved()) return true;
		else return deletedEdges.contains(relation);
	}

	@Override
	public boolean containsVertex(long id) {
		if (super.containsVertex(id)) return true;
		else return graphdb.containsVertexID(id, this);
	}

	@Override
	public void deletedRelation(InternalRelation relation) {
		super.deletedRelation(relation);
		if (relation.isLoaded() && !relation.isInline()) {
			//Only store those deleted edges that matter, i.e. those that we need to erase from memory on their own		
			boolean success = deletedEdges.add(relation);
			assert success;
		}
	}
	

	@Override
	public void addedRelation(InternalRelation relation) {
		super.addedRelation(relation);
		if (!relation.isInline()) {
			//Only store those added edges that matter, i.e. those that we need to erase from memory on their own
			addedEdges.add(relation);
		}
		
	}
	
	@Override
	public void loadedRelation(InternalRelation relation) {
		super.loadedRelation(relation);
	}

	
	/* ---------------------------------------------------------------
	 * Index Handling
	 * ---------------------------------------------------------------
	 */	
	

	@Override
	public TitanVertex getVertex(TitanKey key, Object value) {
		TitanVertex node = super.getVertex(key, value);
		if (node==null && !key.isNew()) {
			//Look up
            value = AttributeUtil.prepareAttribute(value, key.getDataType());
			long[] ids = graphdb.indexRetrieval(value, key, this);
			if (ids.length==0) {
                //TODO Set NO-ENTRY
                return null;
            } else {
				assert ids.length==1;
				InternalTitanVertex n = getExistingVertex(ids[0]);
				addProperty2Index(key, value, n);
				return n;
			}
		} else return node;
	}
	
	@Override
	public long[] getVertexIDsFromDisk(TitanKey type, Object attribute) {
		Preconditions.checkArgument(type.hasIndex(),"Can only retrieve vertices for indexed property types.");
		if (!type.isNew()) {
			long[] ids = graphdb.indexRetrieval(attribute, type, this);
			return ids;
		} else return new long[0];
	}
	
	/* ---------------------------------------------------------------
	 * TitanProperty / TitanRelation Loading
	 * ---------------------------------------------------------------
	 */	

	
	@Override
	public void loadRelations(AtomicQuery query) {
		graphdb.loadRelations(query, this);
	}
	
	@Override
	public AbstractLongList getRawNeighborhood(AtomicQuery query) {
		return graphdb.getRawNeighborhood(query, this);
	}
	
	private void clear() {
		addedEdges=null;
		deletedEdges=null;
	}

	@Override
	public synchronized void commit() {
        Preconditions.checkArgument(isOpen(),"The transaction has already been closed");
        if (!getTxConfiguration().isReadOnly()) {
            try {
                graphdb.save(addedEdges, deletedEdges, this);
            } catch (GraphStorageException e) {
                abort();
                throw e;
            }
        }

        txHandle.commit();
		super.commit();
        clear();
	}

	@Override
	public synchronized void abort() {
        Preconditions.checkArgument(isOpen(),"The transaction has already been closed");
		txHandle.abort();
		super.abort();
        clear();
	}
	
	@Override
	public TransactionHandle getTxHandle() {
		return txHandle;
	}

	@Override
	public boolean hasModifications() {
		return !getTxConfiguration().isReadOnly() && (!deletedEdges.isEmpty() || !addedEdges.isEmpty());
	}

}
