package io.axway.iron.core.internal.entity;

import java.util.*;
import java.util.stream.*;
import com.google.common.collect.Multimap;
import com.google.common.collect.TreeMultimap;
import io.axway.iron.core.internal.definition.entity.RelationCardinality;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder;

import static io.axway.alf.assertion.Assertion.checkArgument;

class RelationMultipleStore extends RelationStore {
    private final Multimap<Long, Long> m_values = TreeMultimap.create(); // tailId -> headIds

    RelationMultipleStore(RelationDefinition relationDefinition) {
        super(relationDefinition);
        checkArgument(relationDefinition.getRelationCardinality() == RelationCardinality.MANY, "Cannot create a RelationMultipleStore for a non MANY relation");
    }

    @Override
    void addRelationMethodHandle(ProxyFactoryBuilder<InstanceProxyContext> callHandlerBuilder, EntityStore<?> headEntityStore) {
        callHandlerBuilder.handle(getRelationDefinition().getRelationMethod(), (context, proxy, method, args) -> {
            Collection<Long> headIds = get(context.getId());
            return Collections.unmodifiableCollection(headIds.stream().map(headEntityStore::getById).collect(Collectors.toList()));
        });
    }

    @Override
    Collection<Long> get(long tailId) {
        return m_values.get(tailId);
    }

    @Override
    void delete(long tailId) {
        clear(tailId);
    }

    @Override
    void recover(long tailId, Object value) {
        Collection<?> values = (Collection<?>) value;
        set(tailId, values.stream().map(o -> ((Number) o).longValue()).collect(Collectors.toList()));
    }

    Collection<Long> set(long tailId, Collection<Long> headIds) {
        Collection<Long> previousHeadIds = m_values.replaceValues(tailId, headIds);
        for (Long previousHeadId : previousHeadIds) {
            m_reverseValues.remove(previousHeadId, tailId);
        }
        for (Long headId : headIds) {
            m_reverseValues.put(headId, tailId);
        }
        return previousHeadIds;
    }

    boolean add(long tailId, long headId) {
        return m_values.put(tailId, headId);
    }

    Collection<Long> addAll(long tailId, Collection<Long> headIds) {
        return headIds.stream().filter(headId -> m_values.put(tailId, headId)).collect(Collectors.toList());
    }

    boolean remove(long tailId, long headId) {
        return m_values.remove(tailId, headId);
    }

    Collection<Long> removeAll(long tailId, Collection<Long> headIds) {
        return headIds.stream().filter(headId -> m_values.remove(tailId, headId)).collect(Collectors.toList());
    }

    Collection<Long> clear(long tailId) {
        Collection<Long> previousHeadIds = m_values.removeAll(tailId);
        for (Long previousHeadId : previousHeadIds) {
            m_reverseValues.remove(previousHeadId, tailId);
        }
        return previousHeadIds;
    }
}
