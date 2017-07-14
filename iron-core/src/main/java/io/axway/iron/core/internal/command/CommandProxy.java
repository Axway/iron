package io.axway.iron.core.internal.command;

import java.util.*;

@SuppressWarnings("WeakerAccess") // this is necessary to avoid classloading/visibility problems for proxy generation in OSGi
public interface CommandProxy {

    <E> Class<E> __commandClass();

    Map<String, Object> __parameters();
}
