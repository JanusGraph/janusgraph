package org.janusgraph.graphdb.types;

import com.google.common.base.Preconditions;
import org.janusgraph.core.VertexLabel;
import org.janusgraph.core.schema.VertexLabelMaker;
import org.janusgraph.graphdb.internal.TitanSchemaCategory;
import org.janusgraph.graphdb.internal.Token;
import org.janusgraph.graphdb.transaction.StandardTitanTx;
import org.janusgraph.graphdb.types.system.SystemTypeManager;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.commons.lang.StringUtils;

import static org.janusgraph.graphdb.types.TypeDefinitionCategory.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class StandardVertexLabelMaker implements VertexLabelMaker {

    private final StandardTitanTx tx;

    private String name;
    private boolean partitioned;
    private boolean isStatic;

    public StandardVertexLabelMaker(StandardTitanTx tx) {
        this.tx = tx;
    }

    public StandardVertexLabelMaker name(String name) {
        //Verify name
        SystemTypeManager.isNotSystemName(TitanSchemaCategory.VERTEXLABEL, name);
        this.name=name;
        return this;
    }


    public String getName() {
        return name;
    }

    @Override
    public StandardVertexLabelMaker partition() {
        partitioned=true;
        return this;
    }

    @Override
    public StandardVertexLabelMaker setStatic() {
        isStatic = true;
        return this;
    }

    @Override
    public VertexLabel make() {
        Preconditions.checkArgument(!partitioned || !isStatic,"A vertex label cannot be partitioned and static at the same time");
        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(PARTITIONED, partitioned);
        def.setValue(STATIC, isStatic);

        return (VertexLabelVertex)tx.makeSchemaVertex(TitanSchemaCategory.VERTEXLABEL,name,def);
    }
}
