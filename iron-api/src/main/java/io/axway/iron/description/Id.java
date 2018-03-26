package io.axway.iron.description;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Used on an entity member, this annotation permits to exposes the internal auto generated id of the instance.<br>
 * It must be used only with a {@code long} data type.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Id {
}
