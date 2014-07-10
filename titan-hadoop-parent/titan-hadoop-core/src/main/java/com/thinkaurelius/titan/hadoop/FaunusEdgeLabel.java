package com.thinkaurelius.titan.hadoop;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.graphdb.schema.EdgeLabelDefinition;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class FaunusEdgeLabel extends FaunusRelationType implements EdgeLabel {

    private final EdgeLabelDefinition definition;

    public static final FaunusEdgeLabel LINK = new FaunusEdgeLabel(
            new EdgeLabelDefinition(Tokens._LINK, FaunusElement.NO_ID,Multiplicity.MULTI,false),false);

    protected FaunusEdgeLabel(EdgeLabelDefinition def, boolean hidden) {
        super(def, hidden);
        this.definition = def;
    }

    @Override
    public boolean isDirected() {
        return definition.isDirected();
    }

    @Override
    public boolean isUnidirected() {
        return definition.isUnidirected();
    }

    @Override
    public boolean isPropertyKey() {
        return false;
    }

    @Override
    public boolean isEdgeLabel() {
        return true;
    }
}
