package io.memris.core;

public interface EntityMetadataProvider {
    <T> EntityMetadata<T> getMetadata(Class<T> entityClass);
}
