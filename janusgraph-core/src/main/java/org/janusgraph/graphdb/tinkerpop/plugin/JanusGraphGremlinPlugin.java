// Copyright 2017 JanusGraph Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.janusgraph.graphdb.tinkerpop.plugin;

import org.apache.tinkerpop.gremlin.jsr223.Customizer;
import org.apache.tinkerpop.gremlin.jsr223.DefaultImportCustomizer;
import org.janusgraph.core.BaseVertexQuery;
import org.janusgraph.core.Cardinality;
import org.janusgraph.core.ConfiguredGraphFactory;
import org.janusgraph.core.EdgeLabel;
import org.janusgraph.core.Idfiable;
import org.janusgraph.core.JanusGraph;
import org.janusgraph.core.JanusGraphComputer;
import org.janusgraph.core.JanusGraphEdge;
import org.janusgraph.core.JanusGraphElement;
import org.janusgraph.core.JanusGraphFactory;
import org.janusgraph.core.JanusGraphIndexQuery;
import org.janusgraph.core.JanusGraphMultiVertexQuery;
import org.janusgraph.core.JanusGraphProperty;
import org.janusgraph.core.JanusGraphQuery;
import org.janusgraph.core.JanusGraphRelation;
import org.janusgraph.core.JanusGraphTransaction;
import org.janusgraph.core.JanusGraphVertex;
import org.janusgraph.core.JanusGraphVertexProperty;
import org.janusgraph.core.JanusGraphVertexQuery;
import org.janusgraph.core.Multiplicity;
import org.janusgraph.core.Namifiable;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.QueryDescription;
import org.janusgraph.core.RelationType;
import org.janusgraph.core.Transaction;
import org.janusgraph.core.TransactionBuilder;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.VertexList;
import org.janusgraph.core.attribute.AttributeSerializer;
import org.janusgraph.core.schema.ConsistencyModifier;
import org.janusgraph.core.schema.DefaultSchemaMaker;
import org.janusgraph.core.schema.EdgeLabelMaker;
import org.janusgraph.core.schema.Index;
import org.janusgraph.core.schema.JanusGraphConfiguration;
import org.janusgraph.core.schema.JanusGraphIndex;
import org.janusgraph.core.schema.JanusGraphManagement;
import org.janusgraph.core.schema.JanusGraphSchemaElement;
import org.janusgraph.core.schema.JanusGraphSchemaType;
import org.janusgraph.core.schema.JobStatus;
import org.janusgraph.core.schema.Mapping;
import org.janusgraph.core.schema.Parameter;
import org.janusgraph.core.schema.PropertyKeyMaker;
import org.janusgraph.core.schema.RelationTypeIndex;
import org.janusgraph.core.schema.RelationTypeMaker;
import org.janusgraph.core.schema.SchemaAction;
import org.janusgraph.core.schema.SchemaInspector;
import org.janusgraph.core.schema.SchemaManager;
import org.janusgraph.core.schema.SchemaStatus;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.example.GraphOfTheGodsFactory;
import org.janusgraph.graphdb.database.management.ManagementSystem;
import org.janusgraph.graphdb.management.ConfigurationManagementGraph;

import java.lang.reflect.Method;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (https://markorodriguez.com)
 */
public class JanusGraphGremlinPlugin extends JanusGraphGremlinDriverPlugin {

    private static final String NAME = "janusgraph.imports";

    private static final Set<Class> CLASS_IMPORTS = new LinkedHashSet<>();
    private static final Set<Enum> ENUM_IMPORTS = new LinkedHashSet<>();
    private static final Set<Method> METHOD_IMPORTS = new LinkedHashSet<>();

    static {
        /////////////
        // CLASSES //
        /////////////

        CLASS_IMPORTS.add(BaseVertexQuery.class);
        CLASS_IMPORTS.add(Cardinality.class);
        CLASS_IMPORTS.add(ConfiguredGraphFactory.class);
        CLASS_IMPORTS.add(EdgeLabel.class);
        CLASS_IMPORTS.add(Idfiable.class);
        CLASS_IMPORTS.add(JanusGraph.class);
        CLASS_IMPORTS.add(JanusGraphComputer.class);
        CLASS_IMPORTS.add(JanusGraphEdge.class);
        CLASS_IMPORTS.add(JanusGraphElement.class);
        CLASS_IMPORTS.add(JanusGraphFactory.class);
        CLASS_IMPORTS.add(JanusGraphIndexQuery.class);
        CLASS_IMPORTS.add(JanusGraphMultiVertexQuery.class);
        CLASS_IMPORTS.add(JanusGraphProperty.class);
        CLASS_IMPORTS.add(JanusGraphQuery.class);
        CLASS_IMPORTS.add(JanusGraphRelation.class);
        CLASS_IMPORTS.add(JanusGraphTransaction.class);
        CLASS_IMPORTS.add(JanusGraphVertex.class);
        CLASS_IMPORTS.add(JanusGraphVertexProperty.class);
        CLASS_IMPORTS.add(JanusGraphVertexQuery.class);
        CLASS_IMPORTS.add(Multiplicity.class);
        CLASS_IMPORTS.add(Namifiable.class);
        CLASS_IMPORTS.add(PropertyKey.class);
        CLASS_IMPORTS.add(QueryDescription.class);
        CLASS_IMPORTS.add(RelationType.class);
        CLASS_IMPORTS.add(Transaction.class);
        CLASS_IMPORTS.add(TransactionBuilder.class);
        CLASS_IMPORTS.add(VertexLabel.class);
        CLASS_IMPORTS.add(VertexList.class);

        CLASS_IMPORTS.add(AttributeSerializer.class);

        CLASS_IMPORTS.add(ConsistencyModifier.class);
        CLASS_IMPORTS.add(DefaultSchemaMaker.class);
        CLASS_IMPORTS.add(EdgeLabelMaker.class);
        CLASS_IMPORTS.add(Index.class);
        CLASS_IMPORTS.add(JanusGraphConfiguration.class);
        CLASS_IMPORTS.add(JanusGraphIndex.class);
        CLASS_IMPORTS.add(JanusGraphManagement.class);
        CLASS_IMPORTS.add(JanusGraphSchemaElement.class);
        CLASS_IMPORTS.add(JanusGraphSchemaType.class);
        CLASS_IMPORTS.add(JobStatus.class);
        CLASS_IMPORTS.add(Mapping.class);
        CLASS_IMPORTS.add(Parameter.class);
        CLASS_IMPORTS.add(PropertyKeyMaker.class);
        CLASS_IMPORTS.add(RelationTypeIndex.class);
        CLASS_IMPORTS.add(RelationTypeMaker.class);
        CLASS_IMPORTS.add(SchemaAction.class);
        CLASS_IMPORTS.add(SchemaInspector.class);
        CLASS_IMPORTS.add(SchemaManager.class);
        CLASS_IMPORTS.add(SchemaStatus.class);
        CLASS_IMPORTS.add(VertexLabelMaker.class);

        CLASS_IMPORTS.add(GraphOfTheGodsFactory.class);
        CLASS_IMPORTS.add(ConfigurationManagementGraph.class);
        CLASS_IMPORTS.add(ManagementSystem.class);

        ///////////
        // ENUMS //
        ///////////

        // also make sure the class is imported for these enums

        Collections.addAll(ENUM_IMPORTS, Multiplicity.values());
        Collections.addAll(ENUM_IMPORTS, Cardinality.values());

    }

    private static final JanusGraphGremlinPlugin instance = new JanusGraphGremlinPlugin();

    public JanusGraphGremlinPlugin() {
        super(NAME, DefaultImportCustomizer.build()
            .addClassImports(CLASS_IMPORTS)
            .addEnumImports(ENUM_IMPORTS)
            .addMethodImports(METHOD_IMPORTS));
    }

    public static JanusGraphGremlinPlugin instance() {
        return instance;
    }

    public JanusGraphGremlinPlugin(String moduleName, Customizer... customizers) {
        super(moduleName, customizers);
    }

    @Override
    public boolean requireRestart() {
        return true;
    }

}
