package io.axway.iron.core.internal.entity;

import java.lang.reflect.Method;
import java.util.*;
import io.axway.iron.core.internal.definition.entity.AttributeDefinition;
import io.axway.iron.core.internal.definition.entity.EntityDefinition;
import io.axway.iron.core.internal.definition.entity.IdDefinition;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.definition.entity.ReverseRelationDefinition;
import io.axway.iron.core.internal.utils.TypeConverter;
import io.axway.iron.core.internal.utils.proxy.ProxyFactory;
import io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder;

class InstanceProxyFactoryBuilder {
    private static final Method PROXY_ID_METHOD;
    private static final Method PROXY_GET_METHOD;
    private static final Method PROXY_SET_METHOD;
    private static final Method PROXY_ENTITY_CLASS_METHOD;

    static {
        try {
            PROXY_ID_METHOD = InstanceProxy.class.getDeclaredMethod("__id");
            PROXY_GET_METHOD = InstanceProxy.class.getDeclaredMethod("__get", String.class);
            PROXY_SET_METHOD = InstanceProxy.class.getDeclaredMethod("__set", String.class, Object.class);
            PROXY_ENTITY_CLASS_METHOD = InstanceProxy.class.getDeclaredMethod("__entityClass");
        } catch (NoSuchMethodException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static <E> ProxyFactory<E, InstanceProxyContext> buildMethodCallHandler(EntityDefinition<E> entityDefinition, Map<Class<?>, EntityStore<?>> entityStores,
                                                                            Map<RelationDefinition, RelationStore> relationStores) {
        Class<E> entityClass = entityDefinition.getEntityClass();
        String entityName = entityDefinition.getEntityName();

        ProxyFactoryBuilder<InstanceProxyContext> callHandlerBuilder = ProxyFactoryBuilder.<InstanceProxyContext>newProxyFactoryBuilder() //
                .defaultObjectEquals() //
                .defaultObjectHashcode() //
                .handleObjectToString((context, proxy, method, args) -> entityName + "(" + context.getId() + ")=" + context.getAttributes()) //
                .handle(PROXY_ID_METHOD, (context, proxy, method, args) -> context.getId()) //
                .handle(PROXY_GET_METHOD, (context, proxy, method, args) -> {
                    String name = (String) args[0];
                    return context.getAttribute(name);
                }) //
                .handle(PROXY_SET_METHOD, (context, proxy, method, args) -> {
                    String name = (String) args[0];
                    return context.setAttribute(name, args[1]);
                }) //
                .handle(PROXY_ENTITY_CLASS_METHOD, (context, proxy, method, args) -> entityClass) //
                ;

        IdDefinition idDefinition = entityDefinition.getIdDefinition();
        if (idDefinition != null) {
            callHandlerBuilder.handle(idDefinition.getIdMethod(), (context, proxy, method, args) -> context.getId());
        }

        for (AttributeDefinition<Object> attributeDefinition : entityDefinition.getAttributes().values()) {
            Method attributeMethod = attributeDefinition.getAttributeMethod();
            String name = attributeDefinition.getAttributeName();
            TypeConverter<?> typeConverter = attributeDefinition.getTypeConverter();
            callHandlerBuilder.handle(attributeMethod, (context, proxy, method, args) -> typeConverter.convert(context.getAttribute(name)));
        }

        for (RelationDefinition relationDefinition : entityDefinition.getRelations().values()) {
            EntityStore<?> headEntityStore = entityStores.get(relationDefinition.getHeadEntityClass());
            relationStores.get(relationDefinition).addRelationMethodHandle(callHandlerBuilder, headEntityStore);
        }

        for (ReverseRelationDefinition reverseRelationDefinition : entityDefinition.getReverseRelations().values()) {
            EntityStore<?> tailEntityStore = entityStores.get(reverseRelationDefinition.getRelationDefinition().getTailEntityClass());
            relationStores.get(reverseRelationDefinition.getRelationDefinition()).addReverseRelationMethodHandle(callHandlerBuilder, tailEntityStore);
        }

        return callHandlerBuilder.build(entityDefinition.getInstanceProxyConstructor());
    }

    private InstanceProxyFactoryBuilder() {
    }
}
