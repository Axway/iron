package io.axway.iron.core.internal.utils;

import java.io.*;
import java.net.URL;
import java.util.*;

public class CompositeClassLoader extends ClassLoader {
    private final List<ClassLoader> m_classLoaders;

    public CompositeClassLoader(List<ClassLoader> classLoaders) {
        m_classLoaders = classLoaders;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        for (ClassLoader cl : m_classLoaders) {
            try {
                return cl.loadClass(name);
            } catch (ClassNotFoundException e) {
                // continue
            }
        }
        throw new ClassNotFoundException(name);
    }

    @Override
    protected Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        Class clazz = findClass(name);
        if (resolve) {
            resolveClass(clazz);
        }
        return clazz;
    }

    @Override
    protected URL findResource(String name) {
        for (ClassLoader cl : m_classLoaders) {
            URL url = cl.getResource(name);
            if (url != null) {
                return url;
            }
        }
        return null;
    }

    @Override
    protected Enumeration<URL> findResources(String name) throws IOException {
        Set<String> urlSet = new HashSet<>();
        List<URL> urlList = new ArrayList<>();
        for (ClassLoader cl : m_classLoaders) {
            Enumeration<URL> urls = cl.getResources(name);
            while (urls.hasMoreElements()) {
                URL url = urls.nextElement();
                if (url != null && urlSet.add(url.toExternalForm())) { //avoid duplicates
                    urlList.add(url);
                }
            }
        }

        return Collections.enumeration(urlList);
    }

    @Override
    public URL getResource(String name) {
        return findResource(name);
    }

    @Override
    public Enumeration<URL> getResources(String name) throws IOException {
        return findResources(name);
    }
}
