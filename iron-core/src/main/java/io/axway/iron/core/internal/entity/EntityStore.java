package io.axway.iron.core.internal.entity;

import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.*;
import java.util.stream.*;
import javax.annotation.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.axway.iron.core.internal.definition.entity.AttributeDefinition;
import io.axway.iron.core.internal.definition.entity.EntityDefinition;
import io.axway.iron.core.internal.definition.entity.IdDefinition;
import io.axway.iron.core.internal.definition.entity.RelationCardinality;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.utils.proxy.ProxyFactory;
import io.axway.iron.error.NonnullConstraintViolationException;
import io.axway.iron.error.StoreException;
import io.axway.iron.error.UniqueConstraintViolationException;
import io.axway.iron.error.UnrecoverableStoreException;
import io.axway.iron.spi.model.snapshot.SerializableAttributeDefinition;
import io.axway.iron.spi.model.snapshot.SerializableEntity;
import io.axway.iron.spi.model.snapshot.SerializableInstance;
import io.axway.iron.spi.model.snapshot.SerializableRelationCardinality;
import io.axway.iron.spi.model.snapshot.SerializableRelationDefinition;

import static io.axway.alf.assertion.Assertion.checkArgument;

public class EntityStore<E> {
    private final EntityDefinition<E> m_entityDefinition;
    private final Class<E> m_entityClass;
    private final String m_entityName;
    private ProxyFactory<E, InstanceProxyContext> m_proxyFactory;

    private final Set<String> m_attributes;
    private final String m_idPropertyName;
    private final Map<String, Map<Object, Long>> m_uniquesIndex;
    private final Set<String> m_nonNullAttributes;
    private final Map<String, RelationStore> m_relationStores;

    private final Map<Long, InstanceProxy> m_instancesById = new TreeMap<>();
    private final AtomicLong m_nextId = new AtomicLong();

    public EntityStore(EntityDefinition<E> entityDefinition, Map<RelationDefinition, RelationStore> relationStores) {
        m_entityDefinition = entityDefinition;
        m_entityClass = entityDefinition.getEntityClass();
        m_entityName = entityDefinition.getEntityName();

        IdDefinition idDefinition = entityDefinition.getIdDefinition();
        m_idPropertyName = idDefinition != null ? idDefinition.getIdName() : null;

        m_attributes = ImmutableSet
                .copyOf(entityDefinition.getAttributes().values().stream().map(AttributeDefinition::getAttributeName).collect(Collectors.toList()));

        ImmutableMap.Builder<String, Map<Object, Long>> uniquesIndex = ImmutableMap.builder();
        entityDefinition.getUniqueConstraints().forEach(uniqueAttribute -> uniquesIndex.put(uniqueAttribute, new HashMap<>()));
        m_uniquesIndex = uniquesIndex.build();

        ImmutableSet.Builder<String> nonNullAttributes = ImmutableSet.builder();
        entityDefinition.getAttributes().values().stream() //
                .filter(attributeDefinition -> !attributeDefinition.isNullable()) //
                .forEach(attributeDefinition -> nonNullAttributes.add(attributeDefinition.getAttributeName()));
        m_nonNullAttributes = nonNullAttributes.build();

        ImmutableMap.Builder<String, RelationStore> relationStoresBuilder = ImmutableMap.builder();
        entityDefinition.getRelations().values().forEach(relationDefinition -> {
            RelationStore relationStore = relationStores.get(relationDefinition);
            relationStoresBuilder.put(relationDefinition.getRelationName(), relationStore);
        });
        m_relationStores = relationStoresBuilder.build();
    }

    public void init(Map<Class<?>, EntityStore<?>> entityStores, Map<RelationDefinition, RelationStore> relationStores) {
        m_proxyFactory = InstanceProxyFactoryBuilder.buildMethodCallHandler(m_entityDefinition, entityStores, relationStores);
    }

    public EntityDefinition<E> getEntityDefinition() {
        return m_entityDefinition;
    }

    public Collection<E> list() {
        return m_instancesById.values().stream().map(m_entityClass::cast).collect(Collectors.toList());
    }

    public <V> E getByUnique(String propertyName, V value) {
        if (propertyName.equals(m_idPropertyName)) {
            return getById((Long) value);
        }
        Map<?, Long> index = m_uniquesIndex.get(propertyName);
        checkArgument(index != null, "Cannot use get method on a non unique property",
                      args -> args.add("entityName", m_entityName).add("propertyName", propertyName));
        Long instanceId = index.get(value);
        if (instanceId != null) {
            InstanceProxy instance = m_instancesById.get(instanceId);
            // instance should always exists if found in the index
            return m_entityClass.cast(instance);
        }

        return null;
    }

    E getById(long id) {
        InstanceProxy instance = m_instancesById.get(id);
        if (instance != null) {
            return m_entityClass.cast(instance);
        } else {
            return null;
        }
    }

    public E newInstance() {
        return newInstance(m_nextId.getAndIncrement());
    }

    public <V> void set(E object, String propertyName, @Nullable V value) {
        InstanceProxy instance = (InstanceProxy) object;
        if (value == null && m_nonNullAttributes.contains(propertyName)) {
            throw new NonnullConstraintViolationException(m_entityName, propertyName);
        }

        set(instance, propertyName, value);
    }

    private Object set(InstanceProxy instance, String propertyName, @Nullable Object value) {
        Object oldValue;
        if (isAttribute(propertyName)) {
            oldValue = instance.__set(propertyName, value);
        } else {
            RelationStore relationStore = m_relationStores.get(propertyName);
            if (relationStore instanceof RelationSimpleStore) {
                RelationSimpleStore relationSimpleStore = (RelationSimpleStore) relationStore;
                if (value != null) {
                    long headInstanceId = value instanceof Long ? (long) value : InstanceProxy.class.cast(value).__id();
                    oldValue = relationSimpleStore.set(instance.__id(), headInstanceId);
                } else {
                    oldValue = relationSimpleStore.remove(instance.__id());
                }
            } else if (relationStore instanceof RelationMultipleStore) {
                RelationMultipleStore relationMultipleStore = (RelationMultipleStore) relationStore;
                if (value != null) {
                    Collection<?> collection = (Collection<?>) value;
                    Collection<Long> idCollection = collection.stream().map(o -> o instanceof Long ? (Long) o : InstanceProxy.class.cast(o).__id())
                            .collect(Collectors.toList());
                    oldValue = relationMultipleStore.set(instance.__id(), idCollection);
                } else {
                    oldValue = relationMultipleStore.clear(instance.__id());
                }
            } else {
                throw new StoreException("Property not found or not updatable", args -> args.add("entityName", m_entityName).add("propertyName", propertyName));
            }
        }
        return oldValue;
    }

    public Runnable insert(E object) {
        InstanceProxy instance = (InstanceProxy) object;
        for (String nonNullAttribute : m_nonNullAttributes) {
            if (instance.__get(nonNullAttribute) == null) {
                throw new NonnullConstraintViolationException(m_entityName, nonNullAttribute);
            }
        }

        indexInstance(instance);
        m_instancesById.put(instance.__id(), instance);

        return () -> delete(object);
    }

    private void indexInstance(InstanceProxy instance) {
        // check and index unique constraint
        boolean shouldRollbackUniqueIndexing = true;
        try {
            for (Map.Entry<String, Map<Object, Long>> e : m_uniquesIndex.entrySet()) {
                String attributeName = e.getKey();
                Object attributeValue = instance.__get(attributeName);
                if (attributeValue != null) {
                    Map<Object, Long> index = e.getValue();
                    if (index.get(attributeValue) != null) {
                        throw new UniqueConstraintViolationException(m_entityName, attributeName, attributeValue);
                    }
                    index.put(attributeValue, instance.__id());
                }
            }
            shouldRollbackUniqueIndexing = false;
        } finally {
            if (shouldRollbackUniqueIndexing) {
                for (Map.Entry<String, Map<Object, Long>> e : m_uniquesIndex.entrySet()) {
                    String attributeName = e.getKey();
                    Object attributeValue = instance.__get(attributeName);
                    if (attributeValue != null) {
                        Map<Object, Long> index = e.getValue();
                        Long indexedId = index.get(attributeValue);
                        if (indexedId != null && indexedId == instance.__id()) {
                            index.remove(attributeName);
                        }
                    }
                }
            }
        }
    }

    public <V> Runnable update(E object, String propertyName, @Nullable V value) {
        InstanceProxy instance = (InstanceProxy) object;
        if (value == null && m_nonNullAttributes.contains(propertyName)) {
            throw new NonnullConstraintViolationException(m_entityName, propertyName);
        }

        long instanceId = instance.__id();
        Map<Object, Long> index = m_uniquesIndex.get(propertyName);
        if (index != null && value != null) {
            Long indexedId = index.get(value);
            if (indexedId != null && indexedId != instanceId) {
                throw new UniqueConstraintViolationException(m_entityName, propertyName, value);
            }
        }

        Object oldValue = set(instance, propertyName, value);

        if (index != null && !Objects.equals(oldValue, value)) {
            if (oldValue != null) {
                index.remove(oldValue);
            }

            if (value != null) {
                index.put(value, instanceId);
            }
        }

        return () -> update(object, propertyName, oldValue);
    }

    public <H> Runnable updateCollectionAdd(E object, String propertyName, H value) {
        return updateCollection(object, propertyName, (relationMultipleStore, tailId) -> {
            long headInstanceId = value instanceof Long ? (Long) value : InstanceProxy.class.cast(value).__id();
            boolean rollbackNeeded = relationMultipleStore.add(tailId, headInstanceId);
            return () -> {
                if (rollbackNeeded) {
                    relationMultipleStore.remove(tailId, headInstanceId);
                }
            };
        });
    }

    public <H> Runnable updateCollectionAddAll(E object, String propertyName, Collection<H> values) {
        return updateCollection(object, propertyName, (relationMultipleStore, tailId) -> {
            Collection<Long> headIds = values.stream().map(o -> o instanceof Long ? (Long) o : InstanceProxy.class.cast(o).__id()).collect(Collectors.toList());
            Collection<Long> addedValues = relationMultipleStore.addAll(tailId, headIds);
            return () -> relationMultipleStore.removeAll(tailId, addedValues);
        });
    }

    public <H> Runnable updateCollectionRemove(E object, String propertyName, H value) {
        return updateCollection(object, propertyName, (relationMultipleStore, tailId) -> {
            long headInstanceId = value instanceof Long ? (Long) value : InstanceProxy.class.cast(value).__id();
            boolean rollbackNeeded = relationMultipleStore.remove(tailId, headInstanceId);
            return () -> {
                if (rollbackNeeded) {
                    relationMultipleStore.add(tailId, headInstanceId);
                }
            };
        });
    }

    public <H> Runnable updateCollectionRemoveAll(E object, String propertyName, Collection<H> values) {
        return updateCollection(object, propertyName, (relationMultipleStore, tailId) -> {
            Collection<Long> headIds = values.stream().map(o -> o instanceof Long ? (Long) o : InstanceProxy.class.cast(o).__id()).collect(Collectors.toList());
            Collection<Long> addedValues = relationMultipleStore.removeAll(tailId, headIds);
            return () -> relationMultipleStore.addAll(tailId, addedValues);
        });
    }

    public <H> Runnable updateCollectionClear(E object, String propertyName) {
        return updateCollection(object, propertyName, (relationMultipleStore, tailId) -> {
            Collection<Long> previousValues = relationMultipleStore.clear(tailId);
            return () -> relationMultipleStore.set(tailId, previousValues);
        });
    }

    private <V, H> Runnable updateCollection(E object, String propertyName, BiFunction<RelationMultipleStore, Long, Runnable> collectionUpdateFunction) {
        RelationStore relationStore = m_relationStores.get(propertyName);
        if (!(relationStore instanceof RelationMultipleStore)) {
            throw new StoreException("Property not found or not updatable through CollectionUpdater",
                                     args -> args.add("entityName", m_entityName).add("propertyName", propertyName));
        }
        RelationMultipleStore relationMultipleStore = (RelationMultipleStore) relationStore;

        InstanceProxy instance = (InstanceProxy) object;

        return collectionUpdateFunction.apply(relationMultipleStore, instance.__id());
    }

    public Runnable delete(E object) {
        InstanceProxy instance = (InstanceProxy) object;

        for (RelationStore relationStore : m_relationStores.values()) {
            relationStore.delete(instance.__id());
        }

        for (Map.Entry<String, Map<Object, Long>> e : m_uniquesIndex.entrySet()) {
            String attributeName = e.getKey();
            Object attributeValue = instance.__get(attributeName);
            if (attributeValue != null) {
                Map<Object, Long> index = e.getValue();
                index.remove(attributeValue);
            }
        }

        m_instancesById.remove(instance.__id());

        return () -> insert(object);
    }

    private E newInstance(long id) {
        InstanceProxyContext instanceProxyContext = new InstanceProxyContext(id);
        return m_proxyFactory.createProxy(instanceProxyContext);
    }

    private boolean isAttribute(String propertyName) {
        return m_attributes.contains(propertyName);
    }

    public SerializableEntity snapshot() {
        SerializableEntity serializableEntity = new SerializableEntity();
        serializableEntity.setEntityName(m_entityName);
        serializableEntity.setRelations(toSerializableRelationDefinitionMap());
        serializableEntity.setAttributes(toSerializableAttributeDefinitionMap());
        serializableEntity.setUniques(m_entityDefinition.getUniqueConstraints().stream().map(ImmutableList::of).collect(Collectors.toList()));
        serializableEntity.setNextId(m_nextId.get());

        serializableEntity.setInstances(m_instancesById.values().stream().map(instanceProxy -> {
            long id = instanceProxy.__id();

            SerializableInstance serializableInstance = new SerializableInstance();
            serializableInstance.setId(id);
            Map<String, Object> values = new HashMap<>();
            for (String attribute : m_attributes) {
                Object value = instanceProxy.__get(attribute);
                if (value != null) {
                    values.put(attribute, value);
                }
            }

            for (RelationStore relationStore : m_relationStores.values()) {
                String relationName = relationStore.getRelationDefinition().getRelationName();
                Object value = relationStore.get(id);
                if (value != null) {
                    values.put(relationName, value);
                }
            }

            serializableInstance.setValues(values);
            return serializableInstance;
        }).collect(Collectors.toList()));

        return serializableEntity;
    }

    private Map<String, SerializableRelationDefinition> toSerializableRelationDefinitionMap() {
        Map<String, SerializableRelationDefinition> result = new HashMap<>();
        m_entityDefinition.getRelations().forEach((key, value) -> result.put(key, toSerializableRelationDefinition(value)));
        return result;
    }

    private SerializableRelationDefinition toSerializableRelationDefinition(RelationDefinition relationDefinition) {
        SerializableRelationDefinition serializableRelationDefinition = new SerializableRelationDefinition();
        serializableRelationDefinition.setHeadEntityName(relationDefinition.getHeadEntityClass().getName());
        serializableRelationDefinition.setCardinality(mapCardinality(relationDefinition.getRelationCardinality()));
        return serializableRelationDefinition;
    }

    private Map<String, SerializableAttributeDefinition> toSerializableAttributeDefinitionMap() {
        Map<String, SerializableAttributeDefinition> result = new HashMap<>();
        m_entityDefinition.getAttributes().forEach((key, value) -> result.put(key, toSerializableAttributeDefinition(value)));
        return result;
    }

    private static SerializableAttributeDefinition toSerializableAttributeDefinition(AttributeDefinition<Object> attributeDefinition) {
        SerializableAttributeDefinition serializableAttributeDefinition = new SerializableAttributeDefinition();
        serializableAttributeDefinition.setDataType(attributeDefinition.getDataType().getName());
        serializableAttributeDefinition.setNullable(attributeDefinition.isNullable());
        return serializableAttributeDefinition;
    }

    public void recover(SerializableEntity serializableEntity) {
        ensureSnapshotCompliantForRelations(serializableEntity);
        ensureSnapshotCompliantForAttributes(serializableEntity);
        ensureSnapshotCompliantForUniques(serializableEntity);

        m_nextId.set(serializableEntity.getNextId());
        serializableEntity.getInstances().forEach(this::recoverInstance);
    }

    private void ensureSnapshotCompliantForRelations(SerializableEntity serializableEntity) {
        // check that persisted relations from the snapshot are the same than the Java model
        for (Map.Entry<String, SerializableRelationDefinition> e : serializableEntity.getRelations().entrySet()) {
            String relationName = e.getKey();
            RelationDefinition relationDefinition = m_entityDefinition.getRelations().get(relationName);
            if (relationDefinition == null) {
                throw new UnrecoverableStoreException("Snapshot contains an relation which no more exists in the model",
                                                      args -> args.add("entityName", m_entityName).add("relationName", relationName));
            }

            String relationHeadEntityName = relationDefinition.getHeadEntityClass().getName();
            SerializableRelationDefinition serializedRelationDefinition = e.getValue();
            if (!relationHeadEntityName.equals(serializedRelationDefinition.getHeadEntityName())) {
                throw new UnrecoverableStoreException("Relation  data type differs between snapshot and model", //
                                                      args -> args.add("entityName", m_entityName) //
                                                              .add("relationName", relationName) //
                                                              .add("snapshotRelationHeadEntityName", serializedRelationDefinition.getHeadEntityName()) //
                                                              .add("modelRelationHeadEntityName", relationHeadEntityName));
            }

            SerializableRelationCardinality modelCardinality = mapCardinality(relationDefinition.getRelationCardinality());
            if (!Objects.equals(modelCardinality, serializedRelationDefinition.getCardinality())) {
                throw new UnrecoverableStoreException("Relation cardinality is different between snapshot and model", //
                                                      args -> args.add("entityName", m_entityName) //
                                                              .add("relationName", relationName) //
                                                              .add("snapshotRelationCardinality", serializedRelationDefinition.getCardinality()) //
                                                              .add("modelRelationCardinality", modelCardinality));
            }
        }

        // new relation in the model are automatically accepted except if they are have cardinality ONE
        for (Map.Entry<String, RelationDefinition> e : m_entityDefinition.getRelations().entrySet()) {
            String relationName = e.getKey();
            if (!serializableEntity.getRelations().containsKey(relationName)) {
                RelationDefinition relationDefinition = e.getValue();
                if (relationDefinition.getRelationCardinality() == RelationCardinality.ONE) {
                    throw new UnrecoverableStoreException("Relation is not present in the snapshot but has a cardinality of ONE",
                                                          args -> args.add("entityName", m_entityName).add("relationName", relationName));
                }
            }
        }
    }

    private void ensureSnapshotCompliantForAttributes(SerializableEntity serializableEntity) {
        // check that persisted attributes from the snapshot are the same than the Java model
        for (Map.Entry<String, SerializableAttributeDefinition> e : serializableEntity.getAttributes().entrySet()) {
            String attributeName = e.getKey();
            AttributeDefinition<Object> attributeDefinition = m_entityDefinition.getAttributes().get(attributeName);
            if (attributeDefinition == null) {
                throw new UnrecoverableStoreException("Snapshot contains an attribute which no more exists in the model",
                                                      args -> args.add("entityName", m_entityName).add("attributeName", attributeName));
            }

            String attributeDataTypeName = attributeDefinition.getDataType().getName();
            SerializableAttributeDefinition serializedAttributeDefinition = e.getValue();
            if (!attributeDataTypeName.equals(serializedAttributeDefinition.getDataType())) {
                throw new UnrecoverableStoreException("Attribute data type differs between snapshot and model",
                                                      args -> args.add("entityName", m_entityName).add("attributeName", attributeName) //
                                                              .add("snapshotDataType", serializedAttributeDefinition.getDataType()) //
                                                              .add("modelDataType", attributeDataTypeName));
            }

            boolean attributeNullable = attributeDefinition.isNullable();
            boolean serializableAttributeNullable = serializedAttributeDefinition.isNullable();
            if (!attributeNullable && serializableAttributeNullable) {
                throw new UnrecoverableStoreException("Attribute is not nullable in the model, but in the snapshot it can be nullable",
                                                      args -> args.add("entityName", m_entityName).add("attributeName", attributeName));
            }
        }

        // new attributes in the model are automatically accepted except if they are not nullable
        for (Map.Entry<String, AttributeDefinition<Object>> e : m_entityDefinition.getAttributes().entrySet()) {
            String attributeName = e.getKey();
            if (!serializableEntity.getAttributes().containsKey(attributeName)) {
                AttributeDefinition<Object> attributeDefinition = e.getValue();
                if (!attributeDefinition.isNullable()) {
                    throw new UnrecoverableStoreException("Attribute is not present in the snapshot but is not nullable",
                                                          args -> args.add("entityName", m_entityName).add("attributeName", attributeName));
                }
            }
        }
    }

    private void ensureSnapshotCompliantForUniques(SerializableEntity serializableEntity) {
        // check that unique constraints of the model are present in snapshot
        // removed unique constraints are automatically accepted
        for (String unique : m_entityDefinition.getUniqueConstraints()) {
            List<String> modelConstraint = ImmutableList.of(unique);
            if (!serializableEntity.getUniques().contains(modelConstraint)) {
                throw new UnrecoverableStoreException("Model defined a new unique constraint which don't exists in the snapshot",
                                                      args -> args.add("entityName", m_entityName).add("uniqueConstraintDefinition", modelConstraint));
            }
        }
    }

    private void recoverInstance(SerializableInstance serializableInstance) {
        long id = serializableInstance.getId();

        if (id >= m_nextId.get()) {
            throw new UnrecoverableStoreException("Instance id is greater than or equals to nextId",
                                                  args -> args.add("instanceId", id).add("nextId", m_nextId.get()));
        }

        E object = newInstance(id);

        Map<String, Object> values = serializableInstance.getValues();
        InstanceProxy instance = (InstanceProxy) object;
        for (String attribute : m_attributes) {
            Object value = values.get(attribute);
            if (value != null) {
                instance.__set(attribute, value);
            }
        }

        for (RelationStore relationStore : m_relationStores.values()) {
            String relationName = relationStore.getRelationDefinition().getRelationName();
            Object value = values.get(relationName);

            if (value != null) {
                relationStore.recover(id, value);
            }
        }
        // TODO ensure head exists in a 2nd recovery phase

        insert(object);
    }

    private SerializableRelationCardinality mapCardinality(RelationCardinality cardinality) {
        return SerializableRelationCardinality.valueOf(cardinality.name());
    }
}
