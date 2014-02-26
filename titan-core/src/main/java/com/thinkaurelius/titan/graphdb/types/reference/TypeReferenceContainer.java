package com.thinkaurelius.titan.graphdb.types.reference;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.internal.TitanTypeCategory;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.graphdb.types.system.SystemRelationType;
import com.thinkaurelius.titan.graphdb.types.system.SystemTypeManager;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.Vertex;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import javax.json.*;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeReferenceContainer implements TypeInspector {

    private final LongObjectOpenHashMap typesById = new LongObjectOpenHashMap();
    private final Map<String,TitanTypeReference> typesByName = new HashMap<String, TitanTypeReference>();

    public TypeReferenceContainer() {

    }

    public TypeReferenceContainer(TitanGraph graph) {
        TitanTransaction tx = null;
        try {
            tx = graph.newTransaction();
            for (Class<? extends TitanType> clazz : new Class[]{TitanKey.class, TitanLabel.class}) {
                for (TitanType type : tx.getTypes(clazz)) {
                    TitanTypeReference ref;
                    if (clazz==TitanKey.class)
                        ref = new TitanKeyReference((TitanKey)type);
                    else
                        ref = new TitanLabelReference((TitanLabel)type);
                    add(ref);
                }
            }
        } finally {
            if (tx!=null) tx.commit();
        }
    }

    public TypeReferenceContainer(String filename) {
        this(getJsonArray(filename));
    }

    private static final JsonArray getJsonArray(String filename) {
        try {
            JsonReader reader = Json.createReader(new FileReader(filename));
            JsonArray array = reader.readArray();
            reader.close();
            return array;
        } catch (IOException e) {
            throw new RuntimeException("Could not load schema from file: " + filename,e);
        }
    }

    public TypeReferenceContainer(JsonArray input) {
        for (int i=0;i<input.size();i++) {
            add(getFromJson(input.getJsonObject(i)));
        }
    }

    public void add(TitanTypeReference type) {
        Preconditions.checkArgument(!typesById.containsKey(type.getID()));
        typesById.put(type.getID(),type);
        Preconditions.checkArgument(!typesByName.containsKey(type.getName()));
        typesByName.put(type.getName(),type);
    }

    public boolean containsType(long id) {
        return typesById.containsKey(id) || SystemTypeManager.isSystemType(id);
    }

    @Override
    public TitanType getExistingType(long id) {
        SystemRelationType st = SystemTypeManager.getSystemType(id);
        if (st!=null) return st;

        Object type = typesById.get(id);
        Preconditions.checkArgument(type!=null,"Type could not be found for id: %s",id);
        return (TitanType)type;
    }

    @Override
    public boolean containsType(String name) {
        return typesByName.containsKey(name) || SystemTypeManager.isSystemType(name);
    }

    @Override
    public TitanType getType(String name) {
        TitanType type = SystemTypeManager.getSystemType(name);
        if (type==null) type = typesByName.get(name);
        return type;
    }

    public void exportToFile(String filename) {
        Preconditions.checkArgument(!(new File(filename).exists()),"A file with the given name already exists: %",filename);
        try {
            JsonWriter writer = Json.createWriter(new FileWriter(filename));
            writer.writeArray(exportToJson());
            writer.close();
        } catch (IOException e) {
            throw new RuntimeException("Could not save schema to file: " + filename,e);
        }
    }

    private static final String NAME_KEY = "name";
    private static final String TYPE_KEY = "type";
    private static final String ID_KEY = "id";
    private static final Set<String> META_KEYS = ImmutableSet.of(NAME_KEY, TYPE_KEY, ID_KEY);

    private static final String INDEX_NAME = "indexname";
    private static final String INDEX_ELEMENTTYPE = "elementtype";
    private static final String INDEX_PARAMETERS = "parameters";
    private static final String PARAMETER_NAME = "name";
    private static final String PARAMETER_VALUE = "value";

    private TitanTypeReference getFromJson(JsonObject input) {
        final long id = input.getJsonNumber(ID_KEY).longValue();
        final String name = input.getString(NAME_KEY);
        final TitanTypeCategory typeClass = TitanTypeCategory.valueOf(input.getString(TYPE_KEY).toUpperCase());
        Preconditions.checkArgument(StringUtils.isNotBlank(name),"Invalid name found for id: %s",id);
        Preconditions.checkArgument(typeClass!=null,"Invalid type found for id: %s",id);
        TypeDefinitionMap definition = new TypeDefinitionMap();
        for (String key : input.keySet()) {
            if (META_KEYS.contains(key)) continue;
            TypeDefinitionCategory tat = TypeDefinitionCategory.valueOf(key.toUpperCase());
            Preconditions.checkArgument(tat!=null,"Unknown attribute: %s",key);
            Object value=null;
            switch(tat) {
                case HIDDEN:
                case MODIFIABLE:
                case UNIDIRECTIONAL:
                    value = input.getBoolean(key);
                    break;
                case DATATYPE:
                    try {
                        value = Class.forName(input.getString(key));
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Could not find attribute class" + input.getString(key), e);
                    }
                    break;
                case SORT_ORDER:
                    value = Order.valueOf(input.getString(key));
                    break;
                case UNIQUENESS:
                case UNIQUENESS_LOCK:
                    JsonArray arr = input.getJsonArray(key);
                    boolean[] bools = new boolean[arr.size()];
                    for (int i=0;i<bools.length;i++) bools[i]=arr.getBoolean(i);
                    value = bools;
                    break;
                case SORT_KEY:
                case SIGNATURE:
                    JsonArray names = input.getJsonArray(key);
                    long[] ids = new long[names.size()];
                    for (int i=0;i<ids.length;i++)
                        ids[i]=getType(names.getString(i)).getID();
                    value = ids;
                    break;
                case INDEXES:
                    arr = input.getJsonArray(key);
                    IndexType[] indexes = new IndexType[arr.size()];
                    for (int i=0;i< indexes.length;i++) {
                        JsonObject obj = arr.getJsonObject(i);
                        String elementType = obj.getString(INDEX_ELEMENTTYPE);
                        Class<? extends Element> element=null;
                        if (elementType.equalsIgnoreCase("vertex")) element = Vertex.class;
                        else if (elementType.equalsIgnoreCase("edge")) element = Edge.class;
                        Preconditions.checkArgument(element!=null,"Invalid element type: " + elementType);
                        indexes[i]=new IndexType(obj.getString(INDEX_NAME),element);
                    }
                    value = indexes;
                    break;
                case INDEX_PARAMETERS:
                    arr = input.getJsonArray(key);
                    IndexParameters[] paras = new IndexParameters[arr.size()];
                    for (int i=0;i<paras.length;i++) {
                        JsonObject obj = arr.getJsonObject(i);
                        JsonArray parameters = obj.getJsonArray(INDEX_PARAMETERS);
                        Parameter[] ps = new Parameter[parameters.size()];
                        for (int j=0;j<ps.length;j++) {
                            JsonObject p = parameters.getJsonObject(j);
                            ps[j]=Parameter.of(p.getString(PARAMETER_NAME),p.getString(PARAMETER_VALUE));
                        }
                        paras[i]=new IndexParameters(obj.getString(INDEX_NAME),ps);
                    }
                    value = paras;
                    break;
            }
            Preconditions.checkNotNull(value);
            definition.setValue(tat,value);
        }

//        for (TypeAttributeType key : new TypeAttributeType[]{TypeAttributeType.SIGNATURE,TypeAttributeType.SORT_KEY}) {
//            if (definition.getValue(key)==null) definition.setValue(key,new long[0]);
//        }
//        if (definition.getValue(TypeAttributeType.INDEXES)==null) {
//            definition.setValue(TypeAttributeType.INDEXES,new IndexType[0]);
//        }
//        if (definition.getValue(TypeAttributeType.INDEX_PARAMETERS)==null) {
//            definition.setValue(TypeAttributeType.INDEX_PARAMETERS,new IndexParameters[0]);
//        }

        switch(typeClass) {
            case KEY: return new TitanKeyReference(id,name,definition);
            case LABEL: return new TitanLabelReference(id,name,definition);
            default: throw new AssertionError("Unknown type class: " + typeClass);
        }
    }

    public JsonArray exportToJson() {
        JsonArrayBuilder result = Json.createArrayBuilder();
        for (TitanTypeReference type : typesWithoutKey()) result.add(exportToJson(type));
        for (TitanTypeReference type : typesWithKey()) result.add(exportToJson(type));
        return result.build();
    }

    private JsonObject exportToJson(TitanTypeReference type) {
        JsonObjectBuilder result = Json.createObjectBuilder();
        result.add(ID_KEY,type.getID());
        result.add(NAME_KEY, type.getName());
        TitanTypeCategory typeClass = type.isPropertyKey()? TitanTypeCategory.KEY: TitanTypeCategory.LABEL;
        result.add(TYPE_KEY, typeClass.toString().toLowerCase());
        for (Map.Entry<TypeDefinitionCategory,Object> def : type.getDefinition().entrySet()) {
            String name = def.getKey().toString().toLowerCase();
            Object value = def.getValue();
            switch(def.getKey()) {
                case HIDDEN:
                case MODIFIABLE:
                case UNIDIRECTIONAL:
                    result.add(name, ((Boolean) value).booleanValue());
                    break;
                case DATATYPE:
                    result.add(name,((Class<?>)value).getName());
                    break;
                case SORT_ORDER:
                    result.add(name,((Order)value).toString());
                    break;
                case UNIQUENESS:
                case UNIQUENESS_LOCK:
                    boolean[] bools = (boolean[])value;
                    assert bools.length==2;
                    JsonArrayBuilder arr = Json.createArrayBuilder();
                    for (int i=0;i<bools.length;i++) arr.add(bools[i]);
                    result.add(name,arr.build());
                    break;
                case SORT_KEY:
                case SIGNATURE:
                    long[] ids = (long[])value;
                    arr = Json.createArrayBuilder();
                    for (int i=0;i<ids.length;i++) arr.add(getExistingType(ids[i]).getName());
                    result.add(name,arr.build());
                    break;
                case INDEXES:
                    IndexType[] indexes = (IndexType[])value;
                    arr = Json.createArrayBuilder();
                    for (int i=0;i< indexes.length;i++) {
                        arr.add(Json.createObjectBuilder().add(INDEX_NAME,indexes[i].getIndexName())
                                .add(INDEX_ELEMENTTYPE,indexes[i].getElementType().getSimpleName().toLowerCase())
                                .build());
                    }
                    result.add(name,arr.build());
                    break;
                case INDEX_PARAMETERS:
                    IndexParameters[] paras = (IndexParameters[])value;
                    arr = Json.createArrayBuilder();
                    for (int i=0;i<paras.length;i++) {
                        Parameter[] ps = paras[i].getParameters();
                        JsonArrayBuilder parameters = Json.createArrayBuilder();
                        for (Parameter p : ps) {
                            parameters.add(Json.createObjectBuilder().add(PARAMETER_NAME,p.getKey())
                                                    .add(PARAMETER_VALUE,p.getValue().toString()).build());
                        }

                        arr.add(Json.createObjectBuilder().add(INDEX_NAME, paras[i].getIndexName())
                                .add(INDEX_PARAMETERS, parameters)
                                .build());
                    }
                    result.add(name,arr.build());
                    break;
            }
        }
        return result.build();
    }


    public void installInGraph(TitanGraph graph) {
        StandardTitanTx tx = null;
        try {
            tx = (StandardTitanTx)graph.newTransaction();
            Map<Long,Long> typeMapping = new HashMap<Long, Long>(typesByName.size());
            for (TitanTypeReference baseType : typesWithoutKey()) {
                TitanType newType;
                if (baseType instanceof TitanKey) {
                    newType = tx.makePropertyKey(baseType.getName(),baseType.getDefinition());
                } else {
                    assert baseType instanceof TitanLabel;
                    newType = tx.makeEdgeLabel(baseType.getName(),baseType.getDefinition());
                }
                Preconditions.checkNotNull(newType);
                typeMapping.put(baseType.getID(),newType.getID());
            }
            for (TitanTypeReference baseType : typesWithKey()) {
                //Remap key
                String name = baseType.getName();
                TypeDefinitionMap definition = new TypeDefinitionMap(baseType.getDefinition());
                for (TypeDefinitionCategory at : new TypeDefinitionCategory[]{
                        TypeDefinitionCategory.SORT_KEY, TypeDefinitionCategory.SIGNATURE}) {
                    long[] arr = definition.getValue(at);
                    if (arr!=null) {
                        long[] newarr = new long[arr.length];
                        for (int i=0;i<arr.length;i++) {
                            newarr[i]=typeMapping.get(arr[i]);
                        }
                        definition.setValue(at, newarr);
                    }
                }

                TitanType newType;
                if (baseType instanceof TitanKey) {
                    newType = tx.makePropertyKey(name,definition);
                } else {
                    assert baseType instanceof TitanLabel;
                    newType = tx.makeEdgeLabel(name,definition);
                }
            }
        } finally {
            if (tx!=null) tx.commit();
        }
    }

    private Iterable<TitanTypeReference> typesWithoutKey() {
        return Iterables.filter(typesByName.values(),typesWithoutKey);
    }

    private Iterable<TitanTypeReference> typesWithKey() {
        return Iterables.filter(typesByName.values(),new Predicate<TitanTypeReference>() {
            @Override
            public boolean apply(@Nullable TitanTypeReference type) {
                return !typesWithoutKey.apply(type);
            }
        });
    }

    private static final Predicate<TitanTypeReference> typesWithoutKey = new Predicate<TitanTypeReference>() {
        @Override
        public boolean apply(@Nullable TitanTypeReference type) {
            return type.getSortKey().length==0 && type.getSignature().length==0;
        }
    };


}
