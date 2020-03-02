# Datatype and Attribute Serializer Configuration

JanusGraph supports a number of classes for attribute values on
properties. JanusGraph efficiently serializes primitives, primitive
arrays and `Geoshape`, `UUID`, `Date`, `ObjectNode` and `ArrayNode`.
JanusGraph supports serializing arbitrary objects as attribute values,
but these require custom serializers to be defined.

To configure a custom attribute class with a custom serializer, follow
these steps:

1.  Implement a custom `AttributeSerializer` for the custom attribute
    class

2.  Add the following configuration options where \[X\] is the custom
    attribute id that must be larger than all attribute ids for already
    configured custom attributes:

    1.  `attributes.custom.attribute[X].attribute-class = [Full attribute class name]`

    2.  `attributes.custom.attribute[X].serializer-class = [Full serializer class name]`

For example, suppose we want to register a special integer attribute
class called `SpecialInt` and have implemented a custom serializer
`SpecialIntSerializer` that implements `AttributeSerializer`. We already
have 9 custom attributes configured in the configuration file, so we
would add the following lines 
```properties
attributes.custom.attribute10.attribute-class = com.example.SpecialInt
attributes.custom.attribute10.serializer-class = com.example.SpecialIntSerializer
```

## Custom Object Serialization

JanusGraph supports arbitrary objects as property attributes and can
serialize such objects to disk. For this default serializer to work for
a custom class, the following conditions must be fulfilled:

-   The class must implement AttributeSerializer

-   The class must have a no-argument constructor

-   The class must implement the `equals(Object)` method

The last requirement is needed because JanusGraph will test both
serialization and deserialization of a custom class before persisting
data to disk.
