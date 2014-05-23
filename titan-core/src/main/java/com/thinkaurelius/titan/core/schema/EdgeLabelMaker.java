package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.Order;
import com.thinkaurelius.titan.core.RelationType;

/**
 * Used to define new {@link com.thinkaurelius.titan.core.EdgeLabel}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface EdgeLabelMaker extends RelationTypeMaker {

    /**
     * Sets the multiplicity of this label. The default multiplicity is {@link com.thinkaurelius.titan.core.Multiplicity#MULTI}.
     * @return
     */
    public EdgeLabelMaker multiplicity(Multiplicity multiplicity);

    /**
     * Configures the label to be directed.
     * <p/>
     * By default, the label is directed.
     *
     * @return this LabelMaker
     * @see com.thinkaurelius.titan.core.EdgeLabel#isDirected()
     */
    public EdgeLabelMaker directed();

    /**
     * Configures the label to be unidirected.
     * <p/>
     * By default, the type is directed.
     *
     * @return this LabelMaker
     * @see com.thinkaurelius.titan.core.EdgeLabel#isUnidirected()
     */
    public EdgeLabelMaker unidirected();

    @Override
    public EdgeLabelMaker signature(RelationType... types);


    /**
     * Defines the {@link com.thinkaurelius.titan.core.EdgeLabel} specified by this LabelMaker and returns the resulting TitanLabel
     *
     * @return
     */
    @Override
    public EdgeLabel make();

}
