package io.axway.iron.core.model.simple;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Unique;

@Entity
public interface SimpleEntity {
    @Unique
    String id();
}
