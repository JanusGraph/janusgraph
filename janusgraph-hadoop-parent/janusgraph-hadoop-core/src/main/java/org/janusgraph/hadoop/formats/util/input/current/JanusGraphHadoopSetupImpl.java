package org.janusgraph.hadoop.formats.util.input.current;

import com.google.common.base.Preconditions;
import org.janusgraph.core.TitanFactory;
import org.janusgraph.core.TitanVertex;
import org.janusgraph.diskstorage.configuration.BasicConfiguration;
import org.janusgraph.graphdb.database.RelationReader;
import org.janusgraph.graphdb.database.StandardTitanGraph;
import org.janusgraph.graphdb.idmanagement.IDManager;
import org.janusgraph.graphdb.internal.TitanSchemaCategory;
import org.janusgraph.graphdb.query.QueryUtil;
import org.janusgraph.graphdb.transaction.StandardTitanTx;
import org.janusgraph.graphdb.types.TypeDefinitionCategory;
import org.janusgraph.graphdb.types.TypeDefinitionMap;
import org.janusgraph.graphdb.types.TypeInspector;
import org.janusgraph.graphdb.types.system.BaseKey;
import org.janusgraph.graphdb.types.system.BaseLabel;
import org.janusgraph.graphdb.types.vertices.TitanSchemaVertex;
import org.janusgraph.hadoop.config.ModifiableHadoopConfiguration;
import org.janusgraph.hadoop.config.TitanHadoopConfiguration;
import org.janusgraph.hadoop.formats.util.input.SystemTypeInspector;
import org.janusgraph.hadoop.formats.util.input.TitanHadoopSetupCommon;
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
