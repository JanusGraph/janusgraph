package com.thinkaurelius.titan.core;

import com.tinkerpop.blueprints.Element;

/**
 * Used to define new {@link TitanKey}s
 *
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public interface KeyMaker extends TypeMaker {

    /**
     * Configures this key to allow multiple properties of this key per vertex.
     * For instance, the property key "phoneNumber" is typically a list()-enabled
     * key to be able to store multiple phone numbers per individual.
     * </p>
     * If a key is configured to be list-valued, the properties must be added
     * with {@link TitanVertex#addProperty(TitanKey, Object)}, retrieved
     * via {@link TitanVertex#getProperties(TitanKey)} and individually deleted.
     *
     * @return this KeyMaker
     */
    public KeyMaker list();

    /**
     * Configures this key to allow at most one property of this key per vertex.
     * In other words, the key uniquely maps onto a value for a particular vertex.
     * For instance, the key "birthdate" is unique for each person.
     * </p>
     * Use {@link TitanVertex#setProperty(TitanKey, Object)} to set or replace
     * this property.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public KeyMaker single(UniquenessConsistency consistency);

    /**
     * As {@link #single(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */
    public KeyMaker single();

    /**
     * Configures this key such that its values are uniquely associated with at most one
     * vertex across the entire graph. For instance, the key "socialSecurityNumber" is a unique
     * key since each SSN belongs to at most one person vertex.
     * </p>
     * This consistency constraint is enforced by the database at runtime.
     * The consistency parameter specifies how this constraint is ensured.
     *
     * @param consistency
     * @return
     */
    public KeyMaker unique(UniquenessConsistency consistency);

    /**
     * As {@link #unique(com.thinkaurelius.titan.core.TypeMaker.UniquenessConsistency)} with
     * the default consistency {@link UniquenessConsistency#LOCK}
     *
     * @return
     */
    public KeyMaker unique();


    /**
     * Configures instances of this type to be indexed for the specified Element type using the <i>standard</i> Titan index.
     * One can either index vertices or edges.
     * <p/>
     * This only applies to property keys.
     * By default, the type is not indexed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex(String, Class)
     * @since 0.3.0
     */
    public KeyMaker indexed(Class<? extends Element> clazz);

    /**
     * Configures instances of this type to be indexed for the specified Element type using the external index with the given name.
     * This index must be configured prior to startup. One can either index vertices or edges.
     * <p/>
     * This only applies to property keys.
     * By default, the type is not indexed.
     *
     * @return this type maker
     * @see com.thinkaurelius.titan.core.TitanKey#hasIndex(String, Class)
     * @since 0.3.0
     */
    public KeyMaker indexed(String indexName, Class<? extends Element> clazz);

    /**
     * Configures the data type for this type.  This only applies to property keys.
     * <p/>
     * Property instances for this key will only accept attribute values that are instances of this class.
     * Every property key must have its data type configured. Setting the data type to Object.class allows
     * any type of attribute but comes at the expense of longer serialization because class information
     * is stored with the attribute value.
     *
     * @param clazz Data type to be configured.
     * @return this type maker
     * @see TitanKey#getDataType()
     */
    public KeyMaker dataType(Class<?> clazz);

    /**
     * Defines the {@link TitanKey} specified by this KeyMaker and returns the resulting TitanKey
     *
     * @return
     */
    @Override
    public TitanKey make();
}
