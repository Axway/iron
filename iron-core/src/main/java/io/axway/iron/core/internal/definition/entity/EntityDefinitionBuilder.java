package io.axway.iron.core.internal.definition.entity;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.axway.iron.core.internal.definition.DataTypeManager;
import io.axway.iron.core.internal.definition.InterfaceValidator;
import io.axway.iron.core.internal.definition.InterfaceVisitor;
import io.axway.iron.core.internal.entity.InstanceProxy;
import io.axway.iron.core.internal.utils.IntrospectionHelper;
import io.axway.iron.core.internal.utils.proxy.ProxyConstructorFactory;
import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;
import io.axway.iron.description.Unique;
import io.axway.iron.description.hook.DSLHelper;
import io.axway.iron.error.InvalidModelException;

import static io.axway.alf.assertion.Assertion.checkState;
import static io.axway.iron.core.internal.definition.entity.RelationCardinality.*;
import static io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder.newProxyFactoryBuilder;

public class EntityDefinitionBuilder {
    private static final Object NO_CONTEXT = new Object();

    private final IntrospectionHelper m_introspectionHelper;
    private final ProxyConstructorFactory m_proxyConstructorFactory;
    private final DataTypeManager m_dataTypeManager;
    private final InterfaceValidator m_interfaceValidator;

    public EntityDefinitionBuilder(IntrospectionHelper introspectionHelper, ProxyConstructorFactory proxyConstructorFactory, DataTypeManager dataTypeManager,
                                   InterfaceValidator interfaceValidator) {
        m_introspectionHelper = introspectionHelper;
        m_proxyConstructorFactory = proxyConstructorFactory;
        m_dataTypeManager = dataTypeManager;
        m_interfaceValidator = interfaceValidator;
    }

    public Map<Class<?>, EntityDefinition<?>> analyzeEntities(Set<Class<?>> entityClasses) {
        ImmutableMap.Builder<Class<?>, EntityDefinition<?>> entityDefinitionsBuilder = ImmutableMap.builder();

        AnalyzeContext context = new AnalyzeContext(entityClasses);
        entityClasses.forEach(entityClass -> {
            EntityDefinition<?> entityDefinition = analyzeEntityClass(context, entityClass);
            entityDefinitionsBuilder.put(entityClass, entityDefinition);
        });

        Map<Class<?>, EntityDefinition<?>> entityDefinitions = entityDefinitionsBuilder.build();

        for (UnboundedReverseRelation unboundedReverseRelation : context.getUnboundedReverseRelations()) {
            ReverseRelationDefinition reverseRelationDefinition = unboundedReverseRelation.getReverseRelationDefinition();
            Class<?> tailEntityClass = unboundedReverseRelation.getTailEntityClass();
            String tailRelationName = unboundedReverseRelation.getTailRelationName();

            EntityDefinition<?> tailEntityDefinition = entityDefinitions.get(tailEntityClass);
            checkState(tailEntityDefinition != null, "Cannot find entity definition", args -> args.add("tailEntityClass", tailEntityClass.getName()));
            RelationDefinition relationDefinition = tailEntityDefinition.getRelations().get(tailRelationName);
            checkState(relationDefinition != null, "Cannot find relation definition",
                       args -> args.add("tailEntityClass", tailEntityClass.getName()).add("tailRelationName", tailRelationName));

            ReverseRelationDefinition existingReverseRelationDefinition = relationDefinition.getReverseRelationDefinition();
            if (existingReverseRelationDefinition != null) {
                throw new InvalidModelException("Relation has already a reverse relation defined so reverse relation cannot be added",
                                                args -> args.add("entityName", tailEntityClass.getName()).add("relationName", tailRelationName)
                                                        .add("reverseRelationName", unboundedReverseRelation.getReverseRelationName()));
            } else {
                relationDefinition.setReverseRelationDefinition(reverseRelationDefinition);
                reverseRelationDefinition.setRelationDefinition(relationDefinition);
            }
        }

        return entityDefinitions;
    }

    private <E> EntityDefinition<E> analyzeEntityClass(AnalyzeContext context, Class<E> entityClass) {

        ImmutableMap.Builder<String, AttributeDefinition<Object>> attributes = ImmutableMap.builder();
        ImmutableMap.Builder<String, RelationDefinition> relations = ImmutableMap.builder();
        ImmutableMap.Builder<String, ReverseRelationDefinition> reverseRelations = ImmutableMap.builder();
        List<String> uniqueConstraints = new ArrayList<>();
        List<IdDefinition> idDefinitions = new ArrayList<>();

        m_interfaceValidator.validate("Entity", entityClass, new InterfaceVisitor() {
            @Override
            public void visitInterface(Class<?> clazz) {
                Entity entityAnnotation = entityClass.getAnnotation(Entity.class);
                if (entityAnnotation == null) {
                    throw new InvalidModelException("Entity interface is not annotated with @Entity",
                                                    args -> args.add("entityClassName", entityClass.getName()));
                }

                if (entityClass.getInterfaces().length > 0) {
                    throw new InvalidModelException("Entity interface extends others interface, which is not supported",
                                                    args -> args.add("entityClassName", entityClass.getName()));
                }
            }

            @Override
            public boolean shouldVisitMethod(Method method) {
                return true;
            }

            @Override
            public <T> void visitMethod(Method method, Class<T> dataType, boolean multiple, boolean nullable) {
                String methodName = method.getName();

                Class<?> returnType = method.getReturnType();

                Annotation uniqueAnnotation = method.getAnnotation(Unique.class);
                Annotation idAnnotation = method.getAnnotation(Id.class);

                Class<?> collectionElementType = null;
                if (Collection.class.isAssignableFrom(returnType)) {
                    collectionElementType = m_introspectionHelper.getParametrizedClass(method.getGenericReturnType(), 0);
                }

                if (context.isEntityClass(dataType)) {
                    // relation case

                    if (idAnnotation != null) {
                        throw new InvalidModelException("Entity relation cannot be annotated with @Id",
                                                        args -> args.add("entityClassName", entityClass.getName()).add("relationMethodName", methodName));
                    }

                    if (uniqueAnnotation != null) {
                        throw new InvalidModelException("Entity relation cannot be annotated with @Unique. Not implemented yet.",
                                                        args -> args.add("entityClassName", entityClass.getName()).add("relationMethodName", methodName));
                    }

                    if (method.isDefault()) {
                        // reverse relation case
                        if (collectionElementType == null) {
                            throw new InvalidModelException("Entity reverse relation must be multiple and return Collection<TailEntity>",
                                                            args -> args.add("entityClassName", entityClass.getName())
                                                                    .add("reverseRelationMethodName", methodName));
                        }

                        reverseRelations.put(methodName, analyzeReverseRelation(context, entityClass, collectionElementType, method));
                    } else {
                        // relation case

                        RelationCardinality cardinality;
                        Class<?> headRelationEntity;
                        if (collectionElementType != null) {

                            cardinality = MANY;
                            headRelationEntity = collectionElementType;
                        } else {
                            cardinality = nullable ? ZERO_ONE : ONE;
                            headRelationEntity = returnType;
                        }

                        relations.put(methodName, new RelationDefinition(method, methodName, entityClass, headRelationEntity, cardinality));
                    }
                } else if (m_dataTypeManager.isValidDataType(returnType)) {
                    // attribute case

                    if (method.isDefault()) {
                        throw new InvalidModelException("Entity attribute must not have a default implementation",
                                                        args -> args.add("entityClassName", entityClass.getName()).add("attributeMethodName", methodName));
                    }

                    if (uniqueAnnotation != null && idAnnotation != null) {
                        throw new InvalidModelException("Entity attribute must not be annotated by both @Id and @Unique",
                                                        args -> args.add("entityClassName", entityClass.getName()).add("attributeMethodName", methodName));
                    }

                    if (uniqueAnnotation != null) {
                        uniqueConstraints.add(methodName);
                    }

                    boolean isId = idAnnotation != null;
                    if (isId && !returnType.equals(long.class)) {
                        throw new InvalidModelException("Entity attribute is annotated with @Id but is not returning long",
                                                        args -> args.add("entityClassName", entityClass.getName()).add("attributeMethodName", methodName));
                    }

                    if (isId) {
                        idDefinitions.add(new IdDefinition(method, methodName));
                    } else {
                        AttributeDefinition<?> attributeDefinition;
                        if (multiple) {
                            attributeDefinition = new AttributeDefinition<>(method, methodName, returnType, nullable,
                                                                            m_dataTypeManager.getCollectionTypeConverter(returnType));
                        } else {
                            attributeDefinition = new AttributeDefinition<>(method, methodName, returnType, nullable,
                                                                            m_dataTypeManager.getTypeConverter(returnType));
                        }
                        //noinspection unchecked
                        attributes.put(methodName, (AttributeDefinition<Object>) attributeDefinition);
                    }
                } else {
                    throw new InvalidModelException("Entity method is not supported",
                                                    args -> args.add("entityClassName", entityClass.getName()).add("methodName", methodName));
                }
            }
        });

        IdDefinition idDefinition = null;
        if (idDefinitions.size() > 1) {
            throw new InvalidModelException("Entity has multiple @Id annotation", args -> args.add("entityClassName", entityClass.getName()));
        } else if (idDefinitions.size() == 1) {
            idDefinition = idDefinitions.get(0);
        }

        Collections.sort(uniqueConstraints);

        Constructor<E> instanceProxyConstructor = m_proxyConstructorFactory.getProxyConstructor(entityClass, InstanceProxy.class);

        return new EntityDefinition<>(entityClass, idDefinition, relations.build(), reverseRelations.build(), attributes.build(),
                                      ImmutableList.copyOf(uniqueConstraints), instanceProxyConstructor);
    }

    private <H, T> ReverseRelationDefinition analyzeReverseRelation(AnalyzeContext context, Class<H> headEntityClass, Class<T> tailEntityClass,
                                                                    Method defaultMethod) {
        H proxy = newProxyFactoryBuilder().handleDefaultMethod(defaultMethod).build(headEntityClass).createProxy(NO_CONTEXT);

        DSLHelperImpl dslHelper = new DSLHelperImpl(m_introspectionHelper);
        DSLHelper.THREAD_LOCAL_DSL_HELPER.set(dslHelper);
        try {
            defaultMethod.invoke(proxy);
        } catch (ReflectiveOperationException e) {
            throw new InvalidModelException(e);
        } finally {
            DSLHelper.THREAD_LOCAL_DSL_HELPER.set(null);
        }
        String tailMethodName = dslHelper.getMethodName();
        if (tailMethodName == null) {
            throw new InvalidModelException("Reverse relation method miss call to the appropriate DSL method",
                                            args -> args.add("entityClassName", defaultMethod.getDeclaringClass().getName())
                                                    .add("reverseRelationMethodName", defaultMethod.getName()));
        }

        Class<?> resolvedTailEntityClass = dslHelper.getTailEntityClass();
        if (!tailEntityClass.equals(resolvedTailEntityClass)) {
            throw new InvalidModelException("Tail entity class mismatch between signature and DSL",
                                            args -> args.add("signatureTailEntityClassName", tailEntityClass.getName())
                                                    .add("dslTailEntityClassName", resolvedTailEntityClass.getName()));
        }

        Method tailMethod;
        try {
            tailMethod = tailEntityClass.getMethod(tailMethodName);
        } catch (NoSuchMethodException e) {
            // should not happen since we retrieve the method name from the method reference
            throw new InvalidModelException(e);
        }

        Class<?> resolvedHeadEntityClass = tailMethod.getReturnType();

        if (Collection.class.isAssignableFrom(resolvedHeadEntityClass)) {
            resolvedHeadEntityClass = m_introspectionHelper.getParametrizedClass(tailMethod.getGenericReturnType(), 0);
        }

        if (!headEntityClass.equals(resolvedHeadEntityClass)) {
            Class<?> resolvedHeadEntityClass0 = resolvedHeadEntityClass;
            throw new InvalidModelException("Head entity class mismatch between signature and DSL",
                                            args -> args.add("signatureTailEntityClassName", headEntityClass.getName())
                                                    .add("dslTailEntityClassName", resolvedHeadEntityClass0.getName()));
        }

        ReverseRelationDefinition reverseRelationDefinition = new ReverseRelationDefinition(defaultMethod);
        context.addUnboundedReverseRelation(new UnboundedReverseRelation(reverseRelationDefinition, defaultMethod.getName(), tailEntityClass, tailMethodName));
        return reverseRelationDefinition;
    }

    private static class AnalyzeContext {
        private final Set<Class<?>> m_entityClasses;
        private final Collection<UnboundedReverseRelation> m_unboundedReverseRelations = new ArrayList<>();

        AnalyzeContext(Set<Class<?>> entityClasses) {
            m_entityClasses = entityClasses;
        }

        boolean isEntityClass(Class<?> clazz) {
            return m_entityClasses.contains(clazz);
        }

        void addUnboundedReverseRelation(UnboundedReverseRelation unboundedReverseRelation) {
            m_unboundedReverseRelations.add(unboundedReverseRelation);
        }

        Collection<UnboundedReverseRelation> getUnboundedReverseRelations() {
            return m_unboundedReverseRelations;
        }
    }

    private static class UnboundedReverseRelation {
        private final ReverseRelationDefinition m_reverseRelationDefinition;
        private final String m_reverseRelationName;
        private final Class<?> m_tailEntityClass;
        private final String m_tailRelationName;

        UnboundedReverseRelation(ReverseRelationDefinition reverseRelationDefinition, String reverseRelationName, Class<?> tailEntityClass,
                                 String tailRelationName) {
            m_reverseRelationDefinition = reverseRelationDefinition;
            m_reverseRelationName = reverseRelationName;
            m_tailEntityClass = tailEntityClass;
            m_tailRelationName = tailRelationName;
        }

        ReverseRelationDefinition getReverseRelationDefinition() {
            return m_reverseRelationDefinition;
        }

        String getReverseRelationName() {
            return m_reverseRelationName;
        }

        Class<?> getTailEntityClass() {
            return m_tailEntityClass;
        }

        String getTailRelationName() {
            return m_tailRelationName;
        }
    }
}
