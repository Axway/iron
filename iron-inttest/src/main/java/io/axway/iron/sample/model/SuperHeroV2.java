package io.axway.iron.sample.model;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;

@Entity
public interface SuperHeroV2 {
    @Id
    long id();

    String nickName();
}
