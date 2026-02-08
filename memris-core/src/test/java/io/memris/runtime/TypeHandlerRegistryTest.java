package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.runtime.handler.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;

import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for TypeHandlerRegistry.
 */
class TypeHandlerRegistryTest {

    private TypeHandlerRegistry registry;

    @BeforeEach
    void setUp() {
        registry = new TypeHandlerRegistry();
    }

    @Test
    @DisplayName("should have default handlers registered")
    void defaultHandlersRegistered() {
        assertThat(List.of(
                registry.hasHandler(TypeCodes.TYPE_LONG),
                registry.hasHandler(TypeCodes.TYPE_INT),
                registry.hasHandler(TypeCodes.TYPE_STRING),
                registry.hasHandler(TypeCodes.TYPE_BOOLEAN),
                registry.hasHandler(TypeCodes.TYPE_DOUBLE),
                registry.hasHandler(TypeCodes.TYPE_BYTE),
                registry.hasHandler(TypeCodes.TYPE_SHORT),
                registry.hasHandler(TypeCodes.TYPE_CHAR),
                registry.hasHandler(TypeCodes.TYPE_FLOAT)
        )).containsOnly(true);
    }

    @Test
    @DisplayName("should get handler by type code")
    void getHandlerByTypeCode() {
        assertThat(List.of(
                registry.getHandler(TypeCodes.TYPE_LONG),
                registry.getHandler(TypeCodes.TYPE_INT),
                registry.getHandler(TypeCodes.TYPE_STRING)
        )).extracting(handler -> handler.getClass().getSimpleName())
                .containsExactly("LongTypeHandler", "IntTypeHandler", "StringTypeHandler");
    }

    @Test
    @DisplayName("should get handler by Java type")
    void getHandlerByJavaType() {
        assertThat(List.of(
                registry.getHandler(Long.class),
                registry.getHandler(Integer.class),
                registry.getHandler(String.class)
        )).extracting(handler -> handler.getClass().getSimpleName())
                .containsExactly("LongTypeHandler", "IntTypeHandler", "StringTypeHandler");
    }

    @Test
    @DisplayName("should return null for unknown type code")
    void unknownTypeCodeReturnsNull() {
        assertThat(registry.getHandler((byte) -1)).isNull();
    }

    @Test
    @DisplayName("should return null for unknown Java type")
    void unknownJavaTypeReturnsNull() {
        assertThat(registry.getHandler(UUID.class)).isNull();
    }

    @Test
    @DisplayName("should check if handler exists by Java type")
    void hasHandlerByJavaType() {
        assertThat(List.of(registry.hasHandler(Long.class), registry.hasHandler(UUID.class)))
                .containsExactly(true, false);
    }

    @Test
    @DisplayName("should check if handler exists by type code")
    void hasHandlerByTypeCode() {
        assertThat(List.of(registry.hasHandler(TypeCodes.TYPE_LONG), registry.hasHandler((byte) -1)))
                .containsExactly(true, false);
    }

    @Test
    @DisplayName("should register custom handler")
    void registerCustomHandler() {
        ByteTypeHandler customHandler = new ByteTypeHandler();
        registry.registerHandler(customHandler);

        assertThat(registry.getHandler(TypeCodes.TYPE_BYTE)).isSameAs(customHandler);
    }

    @Test
    @DisplayName("empty registry should have no handlers")
    void emptyRegistryHasNoHandlers() {
        var emptyRegistry = TypeHandlerRegistry.empty();

        assertThat(List.of(
                emptyRegistry.hasHandler(TypeCodes.TYPE_LONG),
                emptyRegistry.hasHandler(TypeCodes.TYPE_INT)
        )).containsExactly(false, false);
    }

    @Test
    @DisplayName("default registry singleton should be same instance")
    void defaultRegistrySingleton() {
        var registry1 = TypeHandlerRegistry.getDefault();
        var registry2 = TypeHandlerRegistry.getDefault();

        assertThat(registry1).isSameAs(registry2);
    }

    @Test
    @DisplayName("should get all registered type codes")
    void getRegisteredTypeCodes() {
        assertThat(registry.getRegisteredTypeCodes())
                .contains(TypeCodes.TYPE_LONG, TypeCodes.TYPE_INT, TypeCodes.TYPE_STRING);
    }

    @Test
    @DisplayName("should get all registered Java types")
    void getRegisteredJavaTypes() {
        assertThat(registry.getRegisteredJavaTypes())
                .contains(Long.class, Integer.class, String.class);
    }

    @Test
    @DisplayName("should remove handler by type code")
    void removeHandler() {
        var removed = registry.removeHandler(TypeCodes.TYPE_LONG);

        assertThat(List.of(
                removed != null && removed.getClass() == LongTypeHandler.class,
                registry.hasHandler(TypeCodes.TYPE_LONG),
                registry.hasHandler(Long.class)
        )).containsExactly(true, false, false);
    }

    @Test
    @DisplayName("should return null when removing non-existent handler")
    void removeNonExistentHandlerReturnsNull() {
        var removed = registry.removeHandler((byte) -1);
        assertThat(removed).isNull();
    }

    @Test
    @DisplayName("should clear all handlers")
    void clearHandlers() {
        registry.clear();

        assertThat(List.of(
                registry.getRegisteredTypeCodes().isEmpty(),
                registry.getRegisteredJavaTypes().isEmpty()
        )).containsOnly(true);
    }
}
