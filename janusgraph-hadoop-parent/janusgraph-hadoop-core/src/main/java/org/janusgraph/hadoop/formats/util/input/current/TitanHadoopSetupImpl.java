package com.thinkaurelius.titan.hadoop.formats.util.input.current;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.TitanFactory;
import com.thinkaurelius.titan.core.TitanVertex;
import com.thinkaurelius.titan.diskstorage.configuration.BasicConfiguration;
import com.thinkaurelius.titan.graphdb.database.RelationReader;
import com.thinkaurelius.titan.graphdb.database.StandardTitanGraph;
import com.thinkaurelius.titan.graphdb.idmanagement.IDManager;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.query.QueryUtil;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory;
import com.thinkaurelius.titan.graphdb.types.TypeDefinitionMap;
import com.thinkaurelius.titan.graphdb.types.TypeInspector;
import com.thinkaurelius.titan.graphdb.types.system.BaseKey;
import com.thinkaurelius.titan.graphdb.types.system.BaseLabel;
import com.thinkaurelius.titan.graphdb.types.vertices.TitanSchemaVertex;
import com.thinkaurelius.titan.hadoop.config.ModifiableHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.config.TitanHadoopConfiguration;
import com.thinkaurelius.titan.hadoop.formats.util.input.SystemTypeInspector;
import com.thinkaurelius.titan.hadoop.formats.util.input.TitanHadoopSetupCommon;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TitanHadoopSetupImpl extends TitanHadoopSetupCommon {

    private final ModifiableHadoopConfiguration scanConf;
    private final StandardTitanGraph graph;
    private final StandardTitanTx tx;

    public TitanHadoopSetupImpl(final Configuration config) {
        scanConf = ModifiableHadoopConfiguration.of(TitanHadoopConfiguration.MAPRED_NS, config);
        BasicConfiguration bc = scanConf.getTitanGraphConf();
        graph = (StandardTitanGraph) TitanFactory.open(bc);
        tx = (StandardTitanTx)graph.buildTransaction().readOnly().vertexCacheSize(200).start();
    }

    @Override
    public TypeInspector getTypeInspector() {
        //Pre-load schema
        for (TitanSchemaCategory sc : TitanSchemaCategory.values()) {
            for (TitanVertex k : QueryUtil.getVertices(tx, BaseKey.SchemaCategory, sc)) {
                assert k instanceof TitanSchemaVertex;
                TitanSchemaVertex s = (TitanSchemaVertex)k;
                if (sc.hasName()) {
                    String name = s.name();
                    Preconditions.checkNotNull(name);
                }
                TypeDefinitionMap dm = s.getDefinition();
                Preconditions.checkNotNull(dm);
                s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.OUT);
                s.getRelated(TypeDefinitionCategory.TYPE_MODIFIER, Direction.IN);
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
                return typeid == BaseKey.VertexExists.longId();
            }

            @Override
            public boolean isVertexLabelSystemType(long typeid) {
                return typeid == BaseLabel.VertexLabelEdge.longId();
            }

            @Override
            public boolean isTypeSystemType(long typeid) {
                return typeid == BaseKey.SchemaCategory.longId() ||
                        typeid == BaseKey.SchemaDefinitionProperty.longId() ||
                        typeid == BaseKey.SchemaDefinitionDesc.longId() ||
                        typeid == BaseKey.SchemaName.longId() ||
                        typeid == BaseLabel.SchemaDefinitionEdge.longId();
            }
        };
    }

    @Override
    public IDManager getIDManager() {
        return graph.getIDManager();
    }

    @Override
    public RelationReader getRelationReader(long vertexid) {
        return graph.getEdgeSerializer();
    }

    @Override
    public void close() {
        tx.rollback();
        graph.close();
    }

    @Override
    public boolean getFilterPartitionedVertices() {
        return scanConf.get(TitanHadoopConfiguration.FILTER_PARTITIONED_VERTICES);
    }
}
