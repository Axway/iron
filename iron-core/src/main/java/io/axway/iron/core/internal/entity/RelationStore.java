package io.axway.iron.core.internal.entity;

import java.util.*;
import java.util.stream.*;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.definition.entity.ReverseRelationDefinition;
import io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder;

import static io.axway.alf.assertion.Assertion.checkState;
import static io.axway.iron.core.internal.definition.entity.RelationCardinality.MANY;

public abstract class RelationStore {

    public static RelationStore newRelationStore(RelationDefinition relationDefinition) {
        boolean isMultiple = relationDefinition.getRelationCardinality() == MANY;
        return isMultiple ? new RelationMultipleStore(relationDefinition) : new RelationSimpleStore(relationDefinition);
    }

    private final RelationDefinition m_relationDefinition;

    final Multimap<Long, Long> m_reverseValues = TreeMultimap.create(); // headId -> tailIds

    RelationStore(RelationDefinition relationDefinition) {
        m_relationDefinition = relationDefinition;
    }

    RelationDefinition getRelationDefinition() {
        return m_relationDefinition;
    }

    abstract Object get(long tailId);

    abstract void delete(long tailId);

    abstract void recover(long tailId, Object value);

    abstract void addRelationMethodHandle(ProxyFactoryBuilder<InstanceProxyContext> callHandlerBuilder, EntityStore<?> headEntityStore);

    void addReverseRelationMethodHandle(ProxyFactoryBuilder<InstanceProxyContext> callHandlerBuilder, EntityStore<?> tailEntityStore) {
        ReverseRelationDefinition reverseRelationDefinition = m_relationDefinition.getReverseRelationDefinition();
        checkState(reverseRelationDefinition != null, "Relation has no reverse relation",
                   args -> args.add("entityName", tailEntityStore.getEntityDefinition().getEntityName())
                           .add("relationName", m_relationDefinition.getRelationName()));

        callHandlerBuilder.handle(reverseRelationDefinition.getReverseRelationMethod(), (context, proxy, method, args) -> {
            Collection<Long> tailIds = m_reverseValues.get(context.getId());
            return Collections.unmodifiableCollection(tailIds.stream().map(tailEntityStore::getById).collect(Collectors.toList()));
        });
    }
}
