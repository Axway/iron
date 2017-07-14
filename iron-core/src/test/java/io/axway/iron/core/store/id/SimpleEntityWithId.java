package io.axway.iron.core.store.id;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;

@Entity
public interface SimpleEntityWithId {
    @Id
    long id();

    String value();
}
