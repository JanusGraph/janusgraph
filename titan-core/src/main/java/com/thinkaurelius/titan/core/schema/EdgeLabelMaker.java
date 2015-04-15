package com.thinkaurelius.titan.core.schema;

import com.thinkaurelius.titan.core.EdgeLabel;
import com.thinkaurelius.titan.core.Multiplicity;
import com.thinkaurelius.titan.core.PropertyKey;
import com.thinkaurelius.titan.core.RelationType;

/**
 * Used to define new {@link com.thinkaurelius.titan.core.EdgeLabel}s.
 * An edge label is defined by its name, {@link Multiplicity}, its directionality, and its signature - all of which
 * can be specified in this builder.
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface EdgeLabelMaker extends RelationTypeMaker {

    /**
     * Sets the multiplicity of this label. The default multiplicity is {@link com.thinkaurelius.titan.core.Multiplicity#MULTI}.
     * @return this EdgeLabelMaker
     * @see Multiplicity
     */
    public EdgeLabelMaker multiplicity(Multiplicity multiplicity);

    /**
     * Configures the label to be directed.
     * <p/>
     * By default, the label is directed.
     *
     * @return this EdgeLabelMaker
     * @see com.thinkaurelius.titan.core.EdgeLabel#isDirected()
     */
    public EdgeLabelMaker directed();

    /**
     * Configures the label to be unidirected.
     * <p/>
     * By default, the type is directed.
     *
     * @return this EdgeLabelMaker
     * @see com.thinkaurelius.titan.core.EdgeLabel#isUnidirected()
     */
    public EdgeLabelMaker unidirected();


    @Override
    public EdgeLabelMaker signature(PropertyKey... types);


    /**
     * Defines the {@link com.thinkaurelius.titan.core.EdgeLabel} specified by this EdgeLabelMaker and returns the resulting label
     *
     * @return the created {@link EdgeLabel}
     */
    @Override
    public EdgeLabel make();

}
