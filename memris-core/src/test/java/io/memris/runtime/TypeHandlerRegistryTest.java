package io.memris.runtime;

import io.memris.core.TypeCodes;
import io.memris.runtime.handler.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.BeforeEach;

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
        assertThat(registry.hasHandler(TypeCodes.TYPE_LONG)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_INT)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_STRING)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_BOOLEAN)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_DOUBLE)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_BYTE)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_SHORT)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_CHAR)).isTrue();
        assertThat(registry.hasHandler(TypeCodes.TYPE_FLOAT)).isTrue();
    }

    @Test
    @DisplayName("should get handler by type code")
    void getHandlerByTypeCode() {
        TypeHandler<?> longHandler = registry.getHandler(TypeCodes.TYPE_LONG);
        assertThat(longHandler).isInstanceOf(LongTypeHandler.class);

        TypeHandler<?> intHandler = registry.getHandler(TypeCodes.TYPE_INT);
        assertThat(intHandler).isInstanceOf(IntTypeHandler.class);

        TypeHandler<?> stringHandler = registry.getHandler(TypeCodes.TYPE_STRING);
        assertThat(stringHandler).isInstanceOf(StringTypeHandler.class);
    }

    @Test
    @DisplayName("should get handler by Java type")
    void getHandlerByJavaType() {
        TypeHandler<Long> longHandler = registry.getHandler(Long.class);
        assertThat(longHandler).isInstanceOf(LongTypeHandler.class);

        TypeHandler<Integer> intHandler = registry.getHandler(Integer.class);
        assertThat(intHandler).isInstanceOf(IntTypeHandler.class);

        TypeHandler<String> stringHandler = registry.getHandler(String.class);
        assertThat(stringHandler).isInstanceOf(StringTypeHandler.class);
    }

    @Test
    @DisplayName("should return null for unknown type code")
    void unknownTypeCodeReturnsNull() {
        assertThat(registry.getHandler((byte) -1)).isNull();
    }

    @Test
    @DisplayName("should return null for unknown Java type")
    void unknownJavaTypeReturnsNull() {
        assertThat(registry.getHandler(java.util.UUID.class)).isNull();
    }

    @Test
    @DisplayName("should check if handler exists by Java type")
    void hasHandlerByJavaType() {
        assertThat(registry.hasHandler(Long.class)).isTrue();
        assertThat(registry.hasHandler(java.util.UUID.class)).isFalse();
    }

    @Test
    @DisplayName("should check if handler exists by type code")
    void hasHandlerByTypeCode() {
        assertThat(registry.hasHandler(TypeCodes.TYPE_LONG)).isTrue();
        assertThat(registry.hasHandler((byte) -1)).isFalse();
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
        TypeHandlerRegistry emptyRegistry = TypeHandlerRegistry.empty();
        
        assertThat(emptyRegistry.hasHandler(TypeCodes.TYPE_LONG)).isFalse();
        assertThat(emptyRegistry.hasHandler(TypeCodes.TYPE_INT)).isFalse();
    }

    @Test
    @DisplayName("default registry singleton should be same instance")
    void defaultRegistrySingleton() {
        TypeHandlerRegistry registry1 = TypeHandlerRegistry.getDefault();
        TypeHandlerRegistry registry2 = TypeHandlerRegistry.getDefault();
        
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
        assertThat(registry.hasHandler(TypeCodes.TYPE_LONG)).isTrue();
        
        TypeHandler<?> removed = registry.removeHandler(TypeCodes.TYPE_LONG);
        
        assertThat(removed).isInstanceOf(LongTypeHandler.class);
        assertThat(registry.hasHandler(TypeCodes.TYPE_LONG)).isFalse();
        assertThat(registry.hasHandler(Long.class)).isFalse();
    }

    @Test
    @DisplayName("should return null when removing non-existent handler")
    void removeNonExistentHandlerReturnsNull() {
        TypeHandler<?> removed = registry.removeHandler((byte) -1);
        assertThat(removed).isNull();
    }

    @Test
    @DisplayName("should clear all handlers")
    void clearHandlers() {
        assertThat(registry.getRegisteredTypeCodes()).isNotEmpty();
        
        registry.clear();
        
        assertThat(registry.getRegisteredTypeCodes()).isEmpty();
        assertThat(registry.getRegisteredJavaTypes()).isEmpty();
    }
}
