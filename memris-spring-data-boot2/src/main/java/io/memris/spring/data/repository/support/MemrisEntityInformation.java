package io.memris.spring.data.repository.support;

import org.springframework.data.repository.core.EntityInformation;

public final class MemrisEntityInformation<T, ID> implements EntityInformation<T, ID> {
    private final Class<T> domainClass;
    private final String idProperty;
    private final java.lang.reflect.Field idField;

    public MemrisEntityInformation(Class<T> domainClass) {
        this.domainClass = domainClass;
        this.idProperty = resolveIdProperty(domainClass);
        this.idField = resolveIdField(domainClass, idProperty);
        this.idField.setAccessible(true);
    }

    @Override
    public boolean isNew(T entity) {
        return getId(entity) == null;
    }

    @Override
    public ID getId(T entity) {
        try {
            @SuppressWarnings("unchecked")
            var value = (ID) idField.get(entity);
            return value;
        } catch (ReflectiveOperationException e) {
            return null;
        }
    }

    @Override
    public Class<ID> getIdType() {
        @SuppressWarnings("unchecked")
        var idType = (Class<ID>) idField.getType();
        return idType;
    }

    @Override
    public Class<T> getJavaType() {
        return domainClass;
    }

    private static java.lang.reflect.Field resolveIdField(Class<?> domainClass, String idProperty) {
        try {
            return domainClass.getDeclaredField(idProperty);
        } catch (NoSuchFieldException e) {
            throw new IllegalStateException("ID field not found: " + idProperty, e);
        }
    }

    private static String resolveIdProperty(Class<?> domainClass) {
        for (var field : domainClass.getDeclaredFields()) {
            if (hasAnnotation(field, "javax.persistence.Id")) {
                return field.getName();
            }
        }
        throw new IllegalStateException("Missing explicit javax.persistence.Id field on " + domainClass.getName());
    }

    private static boolean hasAnnotation(java.lang.reflect.Field field, String name) {
        for (var annotation : field.getAnnotations()) {
            if (annotation.annotationType().getName().equals(name)) {
                return true;
            }
        }
        return false;
    }
}
