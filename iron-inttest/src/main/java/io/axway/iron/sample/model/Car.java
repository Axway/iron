package io.axway.iron.sample.model;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;

@Entity
public interface Car {
    @Id
    long id();

    String name();
}
