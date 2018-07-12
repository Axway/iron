package io.axway.iron.spi.jackson;

import io.axway.iron.description.Entity;
import io.axway.iron.description.Id;
import io.axway.iron.description.Unique;

@Entity
public interface SimpleEntity {

    @Id
    long id();

    @Unique
    String simpleAttribute();
}
