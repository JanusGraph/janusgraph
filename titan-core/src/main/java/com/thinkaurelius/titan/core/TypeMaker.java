package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Element;

/**
 * TypeMaker is a factory for {@link TitanType}s. TitanTypes can be configured to provide data verification,
 * better storage efficiency, and higher performance. The TitanType defines the schema for all {@link TitanRelation}s
 * of that type.
 * <br />
 * All user defined types are configured using a TypeMaker instance returned by {@link com.thinkaurelius.titan.core.TitanTransaction#makeType()}.
 * Hence, types are defined within the context of a transaction like every other object in a TitanGraph. The TypeMaker
 * is used to create both: property keys and edge labels using either {@link #makePropertyKey()} or {@link #makeEdgeLabel()},
 * respectively. Some of the methods in TypeMaker are only applicable to one or the other.
 * <br />
 * Most configuration options provided by the methods of this class are optional and default values are assumed as
 * documented for the particular methods. However, one must call {@link #name(String)} to define the unqiue name of the type.
 * When defining property keys, one must also configure the data type using {@link #dataType(Class)}.
 *
 * @author Matthias Br&ouml;cheler (http://www.matthiasb.com)
 * @see TitanType
 * @see <a href="https://github.com/thinkaurelius/titan/wiki/Type-Definition-Overview">Titan Type Wiki</a>
 */
public interface TypeMaker {

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
