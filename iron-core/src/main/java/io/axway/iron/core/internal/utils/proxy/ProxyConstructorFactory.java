package io.axway.iron.core.internal.utils.proxy;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.*;
import javax.annotation.*;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.axway.iron.core.internal.utils.CompositeClassLoader;
import io.axway.iron.error.StoreException;

public class ProxyConstructorFactory {
    // cache to avoid to create to many composite class loaders
    private final LoadingCache<CacheKey, ClassLoader> m_compositeClassLoaderCache = CacheBuilder.newBuilder().weakValues()
            .build(new CacheLoader<CacheKey, ClassLoader>() {
                @Override
                public ClassLoader load(CacheKey key) throws Exception {
                    return createClassLoader(key.getClassLoaders());
                }
            });

    public <T> Constructor<T> getProxyConstructor(Class<T> interfaceClass, Class<?>... otherInterfaceClasses) {
        return createProxyConstructor(m_compositeClassLoaderCache, interfaceClass, otherInterfaceClasses);
    }

    static <T> Constructor<T> createProxyConstructor(Class<T> interfaceClass) {
        return createProxyConstructor(null, interfaceClass);
    }

    private static <T> Constructor<T> createProxyConstructor(@Nullable LoadingCache<CacheKey, ClassLoader> cache, Class<T> interfaceClass,
                                                             Class<?>... otherInterfaceClasses) {
        Class<?>[] interfaceClasses = new Class[1 + otherInterfaceClasses.length];
        interfaceClasses[0] = interfaceClass;
        ClassLoader classLoader;
        if (otherInterfaceClasses.length == 0) {
            classLoader = interfaceClass.getClassLoader();
        } else {
            System.arraycopy(otherInterfaceClasses, 0, interfaceClasses, 1, otherInterfaceClasses.length);

            List<ClassLoader> classLoaders = extractClassloaderList(interfaceClasses);
            if (classLoaders.size() == 1) {
                classLoader = classLoaders.get(0);
            } else if (cache != null) {
                classLoader = cache.getUnchecked(new CacheKey(classLoaders));
            } else {
                classLoader = createClassLoader(classLoaders);
            }
        }
        Class<?> uncheckedProxyClass = Proxy.getProxyClass(classLoader, interfaceClasses);
        Class<T> proxyClass = interfaceClass.getClass().cast(uncheckedProxyClass);
        try {
            return proxyClass.getConstructor(InvocationHandler.class);
        } catch (NoSuchMethodException e) {
            throw new StoreException(e);
        }
    }

    private static List<ClassLoader> extractClassloaderList(Class<?>... classes) {
        List<ClassLoader> classLoaders = new ArrayList<>();
        Arrays.stream(classes).map(Class::getClassLoader).filter(cl -> !classLoaders.contains(cl)).forEach(classLoaders::add);
        return ImmutableList.copyOf(classLoaders);
    }

    private static ClassLoader createClassLoader(List<ClassLoader> classLoaders) {
        return new CompositeClassLoader(classLoaders);
    }

    private static final class CacheKey {
        private final List<ClassLoader> m_classLoaders;

        CacheKey(List<ClassLoader> classLoaders) {
            m_classLoaders = classLoaders;
        }

        List<ClassLoader> getClassLoaders() {
            return m_classLoaders;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            CacheKey cacheKey = (CacheKey) o;
            return Objects.equals(m_classLoaders, cacheKey.m_classLoaders);
        }

        @Override
        public int hashCode() {
            return m_classLoaders.hashCode();
        }
    }
}
