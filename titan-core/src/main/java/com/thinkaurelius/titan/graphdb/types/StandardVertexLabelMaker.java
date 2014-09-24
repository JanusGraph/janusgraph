package com.thinkaurelius.titan.graphdb.types;

import com.google.common.base.Preconditions;
import com.thinkaurelius.titan.core.VertexLabel;
import com.thinkaurelius.titan.core.schema.VertexLabelMaker;
import com.thinkaurelius.titan.graphdb.internal.TitanSchemaCategory;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import org.apache.commons.lang.StringUtils;

import static com.thinkaurelius.titan.graphdb.types.TypeDefinitionCategory.*;

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

    public static void checkName(String name) {
        Preconditions.checkArgument(StringUtils.isNotBlank(name), "Need to specify name");
        for (char c : StandardRelationTypeMaker.RESERVED_CHARS)
            Preconditions.checkArgument(name.indexOf(c) < 0, "Name contains reserved character %s: %s", c, name);
    }

    public StandardVertexLabelMaker name(String name) {
        //Verify name
        checkName(name);
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
        // Explicitly paritioned vertex labels are only supported when cluster.partition is true.
        // This check could be made slightly earlier in partition(), but it's safer to maintain
        // state invariants when they're declared in close proximity to each other.
        Preconditions.checkArgument(!partitioned || tx.getGraph().getConfiguration().isClusterPartitioned(),
                "Explicit graph partitioning is required for partitioned vertex labels");

        TypeDefinitionMap def = new TypeDefinitionMap();
        def.setValue(PARTITIONED, partitioned);
        def.setValue(STATIC, isStatic);

        return (VertexLabelVertex)tx.makeSchemaVertex(TitanSchemaCategory.VERTEXLABEL,name,def);
    }
}
