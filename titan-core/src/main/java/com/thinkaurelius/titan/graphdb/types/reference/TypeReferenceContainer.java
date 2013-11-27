package com.thinkaurelius.titan.graphdb.types.reference;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.thinkaurelius.titan.core.*;
import com.thinkaurelius.titan.graphdb.transaction.StandardTitanTx;
import com.thinkaurelius.titan.graphdb.types.*;
import com.thinkaurelius.titan.util.system.ConfigurationUtil;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;

import javax.annotation.Nullable;
import java.io.File;
import java.util.*;

/**
 * @author Matthias Broecheler (me@matthiasb.com)
 */
public class TypeReferenceContainer implements TypeInspector {

    private final LongObjectOpenHashMap typesById = new LongObjectOpenHashMap();
    private final Map<String,TitanTypeReference> typesByName = new HashMap<String, TitanTypeReference>();

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

    public TypeReferenceContainer(String filename) throws ConfigurationException {
        this(getPropertiesConfiguration(filename));
    }

    private static final Configuration getPropertiesConfiguration(String filename) {
        try {
            return new PropertiesConfiguration(filename);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Could not load schema from file: " + filename,e);
        }
    }

    public TypeReferenceContainer(Configuration config) {
        List<String> idsStr = ConfigurationUtil.getUnqiuePrefixes(config);
        List<Long> ids = new ArrayList<Long>(idsStr.size());
        for (String id : idsStr) {
            try {
                ids.add(Long.parseLong(id));
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Could not parse type id: " + id,e);
            }
        }
        for (long id : ids) {
            add(getFromConfiguration(config,id));
        }
    }

    private void add(TitanTypeReference type) {
        Preconditions.checkArgument(!typesById.containsKey(type.getID()));
        typesById.put(type.getID(),type);
        Preconditions.checkArgument(!typesByName.containsKey(type.getName()));
        typesByName.put(type.getName(),type);
    }

    public boolean containsType(long id) {
        return typesById.containsKey(id);
    }

    @Override
    public TitanType getExistingType(long id) {
        Object type = typesById.get(id);
        Preconditions.checkArgument(type!=null,"Type could not be found for id: %s",id);
        return (TitanType)type;
    }

    @Override
    public boolean containsType(String name) {
        return typesByName.containsKey(name);
    }

    @Override
    public TitanType getType(String name) {
        TitanTypeReference type = typesByName.get(name);
        return type;
    }

    public void exportToFile(String filename) {
        Preconditions.checkArgument(!(new File(filename).exists()),"A file with the given name already exists: %",filename);
        PropertiesConfiguration config = new PropertiesConfiguration();
        exportToConfiguration(config);
        try {
            config.save(filename);
        } catch (ConfigurationException e) {
            throw new RuntimeException("Could not save schema to file: " + filename,e);
        }
    }

    public void exportToConfiguration(Configuration config) {
        for (TitanTypeReference type : typesWithoutKey()) exportToConfiguration(config,type);
        for (TitanTypeReference type : typesWithKey()) exportToConfiguration(config,type);
    }

    private static final String NAME_KEY = "name";
    private static final String TYPE_KEY = "type";

    private TitanTypeReference getFromConfiguration(Configuration config, long id) {
        Configuration sub = config.subset(String.valueOf(id));
        Iterator<String> keys = sub.getKeys();
        TypeAttribute.Map definition = new TypeAttribute.Map();
        String name = sub.getString(NAME_KEY);
        Preconditions.checkArgument(StringUtils.isNotBlank(name),"Invalid name found for id: %s",id);
        TitanTypeClass typeClass = TitanTypeClass.valueOf(sub.getString(TYPE_KEY).toUpperCase());
        Preconditions.checkArgument(typeClass!=null,"Invalid type found for id: %s",id);
        while (keys.hasNext()) {
            String key = keys.next();
            if (key.equalsIgnoreCase(NAME_KEY) || key.equalsIgnoreCase(TYPE_KEY)) continue;
            TypeAttributeType tat = TypeAttributeType.valueOf(key.toUpperCase());
            Preconditions.checkArgument(tat!=null,"Unknown attribute: %s",key);
            Object value=null;
            switch(tat) {
                case HIDDEN:
                case MODIFIABLE:
                case UNIDIRECTIONAL:
                    value = sub.getBoolean(key);
                    break;
                case UNIQUENESS:
                case UNIQUENESS_LOCK:
                case STATIC:
                    String[] conv = sub.getStringArray(key);
                    boolean[] bools = new boolean[conv.length];
                    for (int i=0;i<bools.length;i++) bools[i]=Boolean.parseBoolean(conv[i]);
                    value = bools;
                    break;
                case SORT_KEY:
                case SIGNATURE:
                    String[] names = sub.getStringArray(key);
                    long[] ids = new long[names.length];
                    for (int i=0;i<ids.length;i++)
                        ids[i]=getType(names[i]).getID();
                    value = ids;
                    break;
                case INDEXES:
                    names = sub.getStringArray(key);
                    IndexType[] indexes = new IndexType[names.length];
                    for (int i=0;i< indexes.length;i++) indexes[i]=IndexType.valueOf(names[i]);
                    value = indexes;
                    break;
                case DATATYPE:
                    try {
                        value = Class.forName(sub.getString(key));
                    } catch (ClassNotFoundException e) {
                        throw new IllegalArgumentException("Could not find attribute class" + sub.getShort(key), e);
                    }
                    break;
                case SORT_ORDER:
                    value = Order.valueOf(sub.getString(key));
                    break;
                case INDEX_PARAMETERS:
                    names = sub.getStringArray(key);
                    IndexParameters[] paras = new IndexParameters[names.length];
                    for (int i=0;i<paras.length;i++) paras[i]=IndexParameters.valueOf(names[i]);
                    value = paras;
                    break;
            }

            Preconditions.checkNotNull(value);
            definition.setValue(tat,value);
        }

        for (TypeAttributeType key : new TypeAttributeType[]{TypeAttributeType.SIGNATURE,TypeAttributeType.SORT_KEY}) {
            if (definition.getValue(key)==null) definition.setValue(key,new long[0]);
        }
        if (definition.getValue(TypeAttributeType.INDEXES)==null) {
            definition.setValue(TypeAttributeType.INDEXES,new IndexType[0]);
        }
        if (definition.getValue(TypeAttributeType.INDEX_PARAMETERS)==null) {
            definition.setValue(TypeAttributeType.INDEX_PARAMETERS,new IndexParameters[0]);
        }

        switch(typeClass) {
            case KEY: return new TitanKeyReference(id,name,definition);
            case LABEL: return new TitanLabelReference(id,name,definition);
            default: throw new AssertionError("Unknown type class: " + typeClass);
        }
    }

    private void exportToConfiguration(Configuration config, TitanTypeReference type) {
        Configuration sub = config.subset(String.valueOf(type.getID()));
        sub.addProperty(NAME_KEY,type.getName());
        TitanTypeClass typeClass = type.isPropertyKey()?TitanTypeClass.KEY:TitanTypeClass.LABEL;
        sub.addProperty(TYPE_KEY,typeClass.toString().toLowerCase());
        for (TypeAttribute ta : type.getDefinition().getAttributes()) {
            String name = ta.getType().toString().toLowerCase();
            Object value = ta.getValue();
            switch(ta.getType()) {
                case HIDDEN:
                case MODIFIABLE:
                case UNIDIRECTIONAL:
                    value = ((Boolean)value).booleanValue();
                    break;
                case UNIQUENESS:
                case UNIQUENESS_LOCK:
                case STATIC:
                    boolean[] bools = (boolean[])value;
                    String[] conv = new String[bools.length];
                    for (int i=0;i<bools.length;i++) conv[i]=Boolean.toString(bools[i]);
                    value = conv;
                    break;
                case SORT_KEY:
                case SIGNATURE:
                    long[] ids = (long[])value;
                    String[] names = new String[ids.length];
                    for (int i=0;i<ids.length;i++)
                        names[i]=getExistingType(ids[i]).getName();
                    value = names;
                    break;
                case INDEXES:
                    IndexType[] indexes = (IndexType[])value;
                    names = new String[indexes.length];
                    for (int i=0;i< indexes.length;i++) names[i]=indexes[i].toString();
                    value = names;
                    break;
                case DATATYPE:
                    value = ((Class<?>)value).getName();
                    break;
                case SORT_ORDER:
                    value = ((Order)value).toString();
                    break;
                case INDEX_PARAMETERS:
                    IndexParameters[] paras = (IndexParameters[])value;
                    names = new String[paras.length];
                    for (int i=0;i<paras.length;i++) names[i]=paras[i].toString();
                    value = names;
                    break;
            }
            Preconditions.checkNotNull(value);
            sub.setProperty(name,value);
        }
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
                    newType = tx.makePropertyKey(baseType.getName(),baseType.getDefinition());
                }
                Preconditions.checkNotNull(newType);
                typeMapping.put(baseType.getID(),newType.getID());
            }
            for (TitanTypeReference baseType : typesWithKey()) {
                //Remap key
                String name = baseType.getName();
                TypeAttribute.Map definition = new TypeAttribute.Map(baseType.getDefinition());
                for (TypeAttributeType at : new TypeAttributeType[]{
                        TypeAttributeType.SORT_KEY,TypeAttributeType.SIGNATURE}) {
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
                    newType = tx.makePropertyKey(name,definition);
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
