package io.axway.iron.sample.model;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;

@Entity
public interface SuperHeroV1 {
    @Id
    long id();

    String firstName();

    String lastName();
}
