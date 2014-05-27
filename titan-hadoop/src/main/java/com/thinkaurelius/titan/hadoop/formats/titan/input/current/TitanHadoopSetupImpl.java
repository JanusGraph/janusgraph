package com.thinkaurelius.titan.hadoop.formats.titan.input.current;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.diskstorage.StaticBuffer;
import com.thinkaurelius.titan.diskstorage.keycolumnvalue.SliceQuery;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.database.idhandling.IDHandler;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.RelationCategory;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.hadoop.formats.VertexQueryFilter;
import com.thinkaurelius.titan.hadoop.formats.titan.TitanInputFormat;
import com.thinkaurelius.titan.hadoop.formats.titan.input.SystemTypeInspector;
import com.thinkaurelius.titan.hadoop.formats.titan.input.TitanHadoopSetupCommon;
import com.thinkaurelius.titan.hadoop.formats.titan.input.VertexReader;
import com.thinkaurelius.titan.hadoop.formats.titan.util.ConfigurationUtil;
import com.tinkerpop.blueprints.Direction;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanHadoopSetupImpl extends TitanHadoopSetupCommon {

    private final StandardTitanGraph graph;
    private final StandardTitanTx tx;

    public TitanHadoopSetupImpl(final Configuration config) {
        BaseConfiguration titan = ConfigurationUtil.extractConfiguration(config, TitanInputFormat.TITAN_HADOOP_GRAPH_INPUT_TITAN);
        graph = (StandardTitanGraph)TitanFactory.open(titan);
        tx = (StandardTitanTx)graph.buildTransaction().readOnly().setVertexCacheSize(200).start();
   }

    @Override
    public TypeInspector getTypeInspector() {
        //Pre-load schema
        for (TitanSchemaCategory sc : TitanSchemaCategory.values()) {
            for (TitanVertex k : tx.getVertices(BaseKey.SchemaCategory, sc)) {
                assert k instanceof TitanSchemaVertex;
                TitanSchemaVertex s = (TitanSchemaVertex)k;
                String name = s.getName();
                Preconditions.checkNotNull(name);
                TypeDefinitionMap dm = s.getDefinition();
                Preconditions.checkNotNull(dm);
                s.getRelated(TypeDefinitionCategory.CONSISTENCY_MODIFIER, Direction.OUT);
                s.getRelated(TypeDefinitionCategory.CONSISTENCY_MODIFIER, Direction.IN);
            }
        }
        return tx;
    }

    @Override
    public SystemTypeInspector getSystemTypeInspector() {
        return new SystemTypeInspector() {
            @Override
            public boolean isSystemType(long typeid) {
                return IDManager.isSystemRelationTypeId(typeid);
            }

            @Override
            public boolean isVertexExistsSystemType(long typeid) {
                return typeid == BaseKey.VertexExists.getID();
            }

            @Override
            public boolean isTypeSystemType(long typeid) {
                return typeid == BaseKey.SchemaCategory.getID() ||
                        typeid == BaseKey.SchemaDefinitionProperty.getID() ||
                        typeid == BaseKey.SchemaDefinitionDesc.getID() ||
                        typeid == BaseKey.SchemaName.getID() ||
                        typeid == BaseLabel.SchemaDefinitionEdge.getID();
            }
        };
    }

    @Override
    public VertexReader getVertexReader() {
        return new VertexReader() {
            @Override
            public long getVertexId(StaticBuffer key) {
                return graph.getIDManager().getKeyID(key);
            }
        };
    }

    @Override
    public RelationReader getRelationReader(long vertexid) {
        return graph.getEdgeSerializer();
    }

    @Override
    public SliceQuery inputSlice(final VertexQueryFilter inputFilter) {
        if (inputFilter.limit == 0) {
            final StaticBuffer[] endPoints = IDHandler.getBounds(RelationCategory.PROPERTY,false);
            return new SliceQuery(endPoints[0], endPoints[1]).setLimit(Integer.MAX_VALUE);
        } else {
            return super.inputSlice(inputFilter);
        }
    }

    @Override
    public void close() {
        tx.rollback();
        graph.shutdown();
    }
}
