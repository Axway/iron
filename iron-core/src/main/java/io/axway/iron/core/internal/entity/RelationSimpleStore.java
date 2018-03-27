package io.axway.iron.core.internal.entity;

import java.util.*;
import io.axway.iron.core.internal.definition.entity.RelationCardinality;
import io.axway.iron.core.internal.definition.entity.RelationDefinition;
import io.axway.iron.core.internal.utils.proxy.ProxyFactoryBuilder;

import static io.axway.alf.assertion.Assertion.checkArgument;

class RelationSimpleStore extends RelationStore {
    private final Map<Long, Long> m_values = new HashMap<>(); // tailId -> headId

    RelationSimpleStore(RelationDefinition relationDefinition) {
        super(relationDefinition);
        checkArgument(relationDefinition.getRelationCardinality() != RelationCardinality.MANY, "Cannot create a RelationSimpleStore for a MANY relation");
    }

    @Override
    void addRelationMethodHandle(ProxyFactoryBuilder<InstanceProxyContext> callHandlerBuilder, EntityStore<?> headEntityStore) {
        callHandlerBuilder.handle(getRelationDefinition().getRelationMethod(), (context, proxy, method, args) -> {
            Long headId = get(context.getId());
            if (headId != null) {
                return headEntityStore.getById(headId);
            } else {
                return null;
            }
        });
    }

    @Override
    Long get(long tailId) {
        return m_values.get(tailId);
    }

    @Override
    void delete(long tailId) {
        remove(tailId);
    }

    @Override
    void recover(long tailId, Object value) {
        set(tailId, ((Number) value).longValue()); // TODO json hack to be fixed
    }

    Long set(long tailId, long headId) {
        Long previousHeadId = m_values.put(tailId, headId);
        if (previousHeadId != null) {
            if (previousHeadId != headId) {
                m_reverseValues.remove(previousHeadId, tailId);
                m_reverseValues.put(headId, tailId);
            }
        } else {
            m_reverseValues.put(headId, tailId);
        }

        return previousHeadId;
    }

    Long remove(long tailId) {
        Long headId = m_values.remove(tailId);
        if (headId != null) {
            m_reverseValues.remove(headId, tailId);
        }
        return headId;
    }
}
