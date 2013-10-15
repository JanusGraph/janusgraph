package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;

/**
 * TypeMaker is a factory for {@link TitanType}s. TitanTypes can be configured to provide data verification,
 * better storage efficiency, and higher performance. The TitanType defines the schema for all {@link TitanRelation}s
 * of that type.
 * </p>
 * All user defined types are configured using a TypeMaker instance returned by {@link com.thinkaurelius.titan.core.TitanTransaction#makeKey(String)}}
 * or {@link TitanTransaction#makeLabel(String)} where the string parameter is the name of the type to be created.
 * Hence, types are defined within the context of a transaction like every other object in a TitanGraph. The configuration
 * options available when defining a type depend on whether its a key or a label. See {@link KeyMaker} and {@link LabelMaker} for more details.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanType
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-Definition-Overview">Titan Type Wiki</a>
 */
public interface TypeMaker {

    /**
     * Consistency imposed against the underlying storage backend as configured in {@link TypeMaker}.
     */
    public enum UniquenessConsistency {

        /**
         * Does not acquire a lock and hence concurrent transactions may overwrite existing
         * uniqueness relations.
         */
        NO_LOCK,
        /**
         * Acquires a lock to ensure uniqueness consistency.
         */
        LOCK
    }

    public TitanType make();
}
