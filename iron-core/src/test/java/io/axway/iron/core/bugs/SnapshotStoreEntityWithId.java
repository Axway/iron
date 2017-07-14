package io.axway.iron.core.bugs;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;

@Entity
public interface SnapshotStoreEntityWithId {
    @Id
    long id();

    String value();
}
