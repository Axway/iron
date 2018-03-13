package io.axway.iron.core.internal;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.nio.file.InvalidPathException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.function.*;
import java.util.regex.*;
import javax.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.axway.iron.core.StoreManagerFactoryBuilder;
import io.axway.iron.error.ConfigurationException;
import io.axway.iron.spi.serializer.SnapshotSerializer;
import io.axway.iron.spi.serializer.TransactionSerializer;
import io.axway.iron.spi.storage.SnapshotStoreFactory;
import io.axway.iron.spi.storage.TransactionStoreFactory;
import sun.reflect.generics.reflectiveObjects.ParameterizedTypeImpl;

import static java.util.Arrays.*;

public class StoreManagerFactoryBuilderConfigurator {
    private static final Logger LOG = LoggerFactory.getLogger(StoreManagerFactoryBuilderConfigurator.class);
    private static final String ENV_PREFIX = "env:".toLowerCase();
    private static final Pattern VARIABLE_REGEX_PATTERN = Pattern.compile("\\$\\{(.*?)\\}");

    public void fill(StoreManagerFactoryBuilder storeManagerFactoryBuilder, Properties properties) {
        Map<Type, BuilderImplConfig> componentBuilders = findComponentBuilders(properties);
        for (Map.Entry<Type, BuilderImplConfig> entry : componentBuilders.entrySet()) {
            BuilderImplConfig config = entry.getValue();
            Supplier<?> builder = instantiateBuilder(config.supplierClass(), properties, config.baseName());
            buildAndAssign(builder, entry.getKey(), storeManagerFactoryBuilder);
        }
    }

    @SuppressWarnings("unchecked")
    private Map<Type, BuilderImplConfig> findComponentBuilders(Properties properties) {
        Map<Type, BuilderImplConfig> map = new HashMap<>();  // ComponentBuilder -> property name base
        for (String propertyName : properties.stringPropertyNames()) {
            String className = getProperty(properties, propertyName);
            if (className != null) {
                try {
                    Class clazz = Class.forName(className);
                    if (Supplier.class.isAssignableFrom(clazz)) {
                        Type type = ((ParameterizedTypeImpl) clazz.getGenericInterfaces()[0]).getActualTypeArguments()[0];
                        if (map.containsKey(type)) {
                            LOG.warn("A {} is already configured (with {})", type.getTypeName(), map.get(type).supplierClass().getName());
                        }
                        BuilderImplConfig builderImplConfig = new BuilderImplConfig(clazz, propertyName);
                        LOG.info("Adding the configured class in the builder list: {{}: {}}", type.getTypeName(), className);
                        map.put(type, builderImplConfig);
                    }
                } catch (ClassNotFoundException e) {
                    // nothing to do
                }
            }
        }
        return map;
    }

    private Supplier<?> instantiateBuilder(Class<Supplier<?>> componentBuilderClazz, Properties properties, String baseName) {
        try {
            Constructor<Supplier<?>> constructor = componentBuilderClazz.getConstructor();
            Supplier<?> builder = constructor.newInstance();

            for (Method method : componentBuilderClazz.getDeclaredMethods()) {
                if (method.getName().startsWith("set")) {
                    Class<?>[] parameterTypes = method.getParameterTypes();
                    if (parameterTypes.length == 1) {
                        String propertyName = method.getName().substring(3);
                        Object value = getPropertyValue(parameterTypes[0], properties, baseName, propertyName);
                        if (value != null) { // do not call setter if value is null
                            method.invoke(builder, value);
                        } else if (isMandatory(method)) {
                            throw new ConfigurationException("Parameter '" + propertyName + "' for '"+ componentBuilderClazz.getClass().getCanonicalName()+"' is mandatory!");
                        }
                    } else {
                        LOG.error("The configured builder has setters with more than one parameter {{}.{}}", componentBuilderClazz.getName(), method.getName());
                    }
                }
            }
            return builder;
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new ConfigurationException(e);
        }
    }

    private boolean isMandatory(Method method) {
        return stream(method.getParameterAnnotations()[0]).noneMatch(x -> Nullable.class.isAssignableFrom(x.annotationType()));
    }

    @Nullable
    private <T> T getPropertyValue(Class<T> clazz, Properties properties, String baseName, String propertyName) {
        if (clazz.isAssignableFrom(Properties.class)) {
            return extractProperties(clazz, properties, baseName);
        } else {
            String fullName = baseName + "." + camelCaseToLowerCaseUnderscore(propertyName);
            if (clazz.isAssignableFrom(Path.class)) {
                return extractPath(clazz, properties, fullName);
            } else if (clazz.isAssignableFrom(String.class)) {
                return clazz.cast(getProperty(properties, fullName));
            } else {
                return extractStandardTypes(clazz, properties, fullName);
            }
        }
    }

    private String camelCaseToLowerCaseUnderscore(String propertyName) {
        return propertyName.replaceAll("(.)(\\p{Upper})", "$1_$2").toLowerCase();
    }

    @Nullable
    private <T> T extractPath(Class<T> clazz, Properties properties, String fullName) {
        try {
            String value = getProperty(properties, fullName);
            return value != null ? clazz.cast(Paths.get(value)) : null;
        } catch (InvalidPathException e) {
            throw new ConfigurationException(e);
        }
    }

    private <T> T extractProperties(Class<T> clazz, Properties properties, String baseName) {
        Properties value = new Properties();
        int basenameLength = baseName.length() + 1;
        for (String name : properties.stringPropertyNames()) {
            if (name.startsWith(baseName) && !name.equals(baseName)) { // startWith but not the same
                value.put(name.substring(basenameLength), getProperty(properties, name)); // There can't be null value here
            }
        }
        return clazz.cast(value);
    }

    @Nullable
    private <T> T extractStandardTypes(Class<T> clazz, Properties properties, String fullName) {
        try {
            String value = getProperty(properties, fullName);
            return value != null ? clazz.cast(clazz.getDeclaredMethod("valueOf", String.class).invoke(null, value)) : null;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new ConfigurationException(e);
        }
    }

    private String getProperty(Properties properties, String key) {
        String property = properties.getProperty(key);
        if (property != null) {
            if (property.toLowerCase().startsWith(ENV_PREFIX)) {
                // If the value retrieved start with the environment prefix, we retrieve the key value from the System environment
                // e.g: key=ENV:AWS_ACCESS_KEY_ID -> System.getenv("AWS_ACCESS_KEY_ID")
                String envVarName = property.substring(ENV_PREFIX.length());
                property = System.getenv(envVarName);
            } else {
                // If the value retrieved follow the variable pattern, we extract the value, and use it as a key to retrieve the real value.
                // e.g: key=${io.iron.configuration.value}  -> properties.getProperty("io.iron.configuration.value")
                Matcher matcher = VARIABLE_REGEX_PATTERN.matcher(property);
                if (matcher.find()) {
                    property = getProperty(properties, matcher.group(1));  // beware recursive call
                }
            }
        }
        return property;
    }

    @SuppressWarnings("unchecked")
    private void buildAndAssign(Supplier<?> builder, Type type, StoreManagerFactoryBuilder storeManagerFactoryBuilder) {
        Object component = builder.get();
        Class typeClass = (Class) type;
        if (typeClass.isAssignableFrom(TransactionSerializer.class)) {
            storeManagerFactoryBuilder.withTransactionSerializer((TransactionSerializer) component);
        } else if (typeClass.isAssignableFrom(TransactionStoreFactory.class)) {
            storeManagerFactoryBuilder.withTransactionStoreFactory((TransactionStoreFactory) component);
        } else if (typeClass.isAssignableFrom(SnapshotSerializer.class)) {
            storeManagerFactoryBuilder.withSnapshotSerializer((SnapshotSerializer) component);
        } else if (typeClass.isAssignableFrom(SnapshotStoreFactory.class)) {
            storeManagerFactoryBuilder.withSnapshotStoreFactory((SnapshotStoreFactory) component);
        } else {
            LOG.error("The configured component builder does not provide supported class {}", component.getClass().getName());
        }
    }

    private class BuilderImplConfig {
        private Class<Supplier<?>> m_supplierClass;
        private String m_baseName;

        public BuilderImplConfig(Class<Supplier<?>> supplierClass, String baseName) {
            m_supplierClass = supplierClass;
            m_baseName = baseName;
        }

        public Class<Supplier<?>> supplierClass() {
            return m_supplierClass;
        }

        public String baseName() {
            return m_baseName;
        }
    }
}
