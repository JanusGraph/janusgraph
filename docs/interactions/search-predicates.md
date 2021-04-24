# Search Predicates and Mixed Index Data Types

This page lists all of the comparison predicates that JanusGraph supports in global graph search and local traversals.

!!! note 
    Some parts of this section require mixed index, see [mixed index backend](../index-backend/index.md).

## Compare Predicate

The `Compare` enum specifies the following comparison predicates used for index query construction and used in the examples above:

* `eq` (equal)
* `neq` (not equal)
* `gt` (greater than)
* `gte` (greater than or equal)
* `lt` (less than)
* `lte` (less than or equal)

All comparison predicates are supported by String, numeric, Date and Instant data types.
Boolean and UUID data types support the `eq` and `neq` comparison predicates.

`eq` and `neq` can be used on Boolean and UUID.

## Text Predicate

The `Text` enum specifies the [Text Search](../index-backend/text-search.md) used to query for matching text or string values.  We differentiate between two types of predicates:

* Text search predicates which match against the individual words inside a text string after it has been tokenized. These predicates are not case sensitive.
    - `textContains`: is true if (at least) one word inside the text string matches the query string
    - `textNotContains`: is true if no words inside the text string match the query string
    - `textContainsPrefix`: is true if (at least) one word inside the text string begins with the query string
    - `textNotContainsPrefix`: is true if no words inside the text string begin with the query string
    - `textContainsRegex`: is true if (at least) one word inside the text string matches the given regular expression
    - `textNotContainsRegex`: is true if no words inside the text string match the given regular expression
    - `textContainsFuzzy`: is true if (at least) one word inside the text string is similar to the query string (based on Levenshtein edit distance)
    - `textNotContainsFuzzy`:  is true if no words inside the text string are similar to the query string (based on Levenshtein edit distance)
    - `textContainsPhrase`: is true if the text string contains the exact sequence of words in the query string
    - `textNotContainsPhrase`:  is true if the text string does not contain the sequence of words in the query string
* String search predicates which match against the entire string value
    - `textPrefix`: if the string value starts with the given query string
    - `textNotPrefix`: if the string value does not start with the given query string
    - `textRegex`: if the string value matches the given regular expression in its entirety
    - `textNotRegex`: if the string value does not match the given regular expression in its entirety
    - `textFuzzy`: if the string value is similar to the given query string (based on Levenshtein edit distance)
    - `textNotFuzzy`: if the string value is not similar to the given query string (based on Levenshtein edit distance)

See [Text Search](../index-backend/text-search.md) for more information about full-text and string search.

## Geo Predicate

The `Geo` enum specifies geo-location predicates.

* `geoIntersect` which holds true if the two geometric objects have at least one point in common (opposite of `geoDisjoint`).
* `geoWithin` which holds true if one geometric object contains the other.
* `geoDisjoint` which holds true if the two geometric objects have no points in common (opposite of `geoIntersect`).
* `geoContains` which holds true if one geometric object is contained by the other.

See [Geo Mapping](../index-backend/text-search.md#geo-mapping) for more information about geo search.

## Query Examples

The following query examples demonstrate some of the predicates on the tutorial graph.

```groovy
// 1) Find vertices with the name "hercules"
g.V().has("name", "hercules")
// 2) Find all vertices with an age greater than 50
g.V().has("age", gt(50))
// or find all vertices between 1000 (inclusive) and 5000 (exclusive) years of age and order by ascending age
g.V().has("age", inside(1000, 5000)).order().by("age", asc)
// which returns the same result set as the following query but in reverse order
g.V().has("age", inside(1000, 5000)).order().by("age", desc)
// 3) Find all edges where the place is at most 50 kilometers from the given latitude-longitude pair
g.E().has("place", geoWithin(Geoshape.circle(37.97, 23.72, 50)))
// 4) Find all edges where reason contains the word "loves"
g.E().has("reason", textContains("loves"))
// or all edges which contain two words (need to chunk into individual words)
g.E().has("reason", textContains("loves")).has("reason", textContains("breezes"))
// or all edges which contain words that start with "lov"
g.E().has("reason", textContainsPrefix("lov"))
// or all edges which contain words that match the regular expression "br[ez]*s" in their entirety
g.E().has("reason", textContainsRegex("br[ez]*s"))
// or all edges which contain words similar to "love"
g.E().has("reason", textContainsFuzzy("love"))
// 5) Find all vertices older than a thousand years and named "saturn"
g.V().has("age", gt(1000)).has("name", "saturn")
```

## Data Type Support


While JanusGraph's composite indexes support any data type that can be stored in JanusGraph, the mixed indexes are limited to the following data types.

 * Byte
 * Short
 * Integer
 * Long
 * Float
 * Double
 * String
 * Geoshape
 * Date
 * Instant
 * UUID
 
Additional data types will be supported in the future.


## Geoshape Data Type
The Geoshape data type supports representing a point, circle, box, line, polygon, multi-point, multi-line and multi-polygon. Index backends currently support indexing points, circles, boxes, lines, polygons, multi-point, multi-line, multi-polygon and geometry collection.
Geospatial index lookups are only supported via mixed indexes.

To construct a Geoshape use the following methods:

```groovy
 //lat, lng
Geoshape.point(37.97, 23.72)
//lat, lng, radius in km
Geoshape.circle(37.97, 23.72, 50)
//SW lat, SW lng, NE lat, NE lng
Geoshape.box(37.97, 23.72, 38.97, 24.72)
//WKT
Geoshape.fromWkt("POLYGON ((35.4 48.9, 35.6 48.9, 35.6 49.1, 35.4 49.1, 35.4 48.9))")
//MultiPoint
Geoshape.geoshape(Geoshape.getShapeFactory().multiPoint().pointXY(60.0, 60.0).pointXY(120.0, 60.0)
  .build())
//MultiLine
Geoshape.geoshape(Geoshape.getShapeFactory().multiLineString()
  .add(Geoshape.getShapeFactory().lineString().pointXY(59.0, 60.0).pointXY(61.0, 60.0))
  .add(Geoshape.getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0)).build())
//MultiPolygon
Geoshape.geoshape(Geoshape.getShapeFactory().multiPolygon()
  .add(Geoshape.getShapeFactory().polygon().pointXY(59.0, 59.0).pointXY(61.0, 59.0)
    .pointXY(61.0, 61.0).pointXY(59.0, 61.0).pointXY(59.0, 59.0))
  .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0)
    .pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build())
//GeometryCollection
Geoshape.geoshape(Geoshape.getGeometryCollectionBuilder()
  .add(Geoshape.getShapeFactory().pointXY(60.0, 60.0))
  .add(Geoshape.getShapeFactory().lineString().pointXY(119.0, 60.0).pointXY(121.0, 60.0).build())
  .add(Geoshape.getShapeFactory().polygon().pointXY(119.0, 59.0).pointXY(121.0, 59.0)
    .pointXY(121.0, 61.0).pointXY(119.0, 61.0).pointXY(119.0, 59.0)).build())
```

In addition, when importing a graph via GraphSON the geometry may be represented by GeoJSON:

=== "string"
    ```json
    "37.97, 23.72"
    ```

=== "list"
    ```json
    [37.97, 23.72]
    ```

=== "GeoJSON feature"
    ```json
    {
      "type": "Feature",
      "geometry": {
        "type": "Point",
        "coordinates": [125.6, 10.1]
      },
      "properties": {
        "name": "Dinagat Islands"
      }
    }
    ```

=== "GeoJSON geometry"
    ```json
    {
      "type": "Point",
      "coordinates": [125.6, 10.1]
    }
    ```

[GeoJSON](http://geojson.org/) may be specified as Point, Circle, LineString or Polygon. Polygons must be closed.
Note that unlike the JanusGraph API GeoJSON specifies coordinates as lng lat.

## Collections
If you are using [Elasticsearch](../index-backend/elasticsearch.md) then you can index properties with SET and LIST cardinality.
For instance:

```groovy
mgmt = graph.openManagement()
nameProperty = mgmt.makePropertyKey("names").dataType(String.class).cardinality(Cardinality.SET).make()
mgmt.buildIndex("search", Vertex.class).addKey(nameProperty, Mapping.STRING.asParameter()).buildMixedIndex("search")
mgmt.commit()
//Insert a vertex
person = graph.addVertex()
person.property("names", "Robert")
person.property("names", "Bob")
graph.tx().commit()
//Now query it
g.V().has("names", "Bob").count().next() //1
g.V().has("names", "Robert").count().next() //1
```

