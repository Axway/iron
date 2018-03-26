package io.axway.iron.description;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used on any method of an entity or a command that is not part of the entity members or the command parameters.<br>
 * Methods annotated with {@link Transient} must have a default implementation to be usable.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
@Documented
public @interface Transient {
}
