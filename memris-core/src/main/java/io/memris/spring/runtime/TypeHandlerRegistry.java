package io.memris.spring.runtime;

import io.memris.spring.runtime.handlers.BooleanTypeHandler;
import io.memris.spring.runtime.handlers.ByteTypeHandler;
import io.memris.spring.runtime.handlers.CharTypeHandler;
import io.memris.spring.runtime.handlers.DoubleTypeHandler;
import io.memris.spring.runtime.handlers.FloatTypeHandler;
import io.memris.spring.runtime.handlers.IntTypeHandler;
import io.memris.spring.runtime.handlers.LongTypeHandler;
import io.memris.spring.runtime.handlers.ShortTypeHandler;
import io.memris.spring.runtime.handlers.StringTypeHandler;

import java.util.HashMap;
import java.util.Map;

/**
 * Registry for type handlers.
 * 
 * <p>Manages the mapping from type codes to their corresponding handlers.
 * Provides a central place to register and lookup type handlers.
 * 
 * <p>This registry is extensible - new handlers can be registered at runtime
 * to support additional data types without modifying the core kernel.
 * 
 * <p>Example usage:
 * <pre>
 * // Get the default registry
 * TypeHandlerRegistry registry = TypeHandlerRegistry.getDefault();
 * 
 * // Lookup a handler
 * TypeHandler<?> handler = registry.getHandler(TypeCodes.TYPE_LONG);
 * 
 * // Register a custom handler
 * registry.registerHandler(new BigDecimalTypeHandler());
 * </pre>
 */
public class TypeHandlerRegistry {
    
    private final Map<Byte, TypeHandler<?>> handlersByTypeCode = new HashMap<>();
    private final Map<Class<?>, TypeHandler<?>> handlersByJavaType = new HashMap<>();
    
    /**
     * Create a new registry with default handlers registered.
     */
    public TypeHandlerRegistry() {
        registerDefaultHandlers();
    }
    
    /**
     * Create an empty registry (no default handlers).
     */
    public static TypeHandlerRegistry empty() {
        return new TypeHandlerRegistry(false);
    }
    
    /**
     * Get the default shared registry instance.
     */
    public static TypeHandlerRegistry getDefault() {
        return DefaultHolder.INSTANCE;
    }
    
    private TypeHandlerRegistry(boolean registerDefaults) {
        if (registerDefaults) {
            registerDefaultHandlers();
        }
    }
    
    /**
     * Register the default set of type handlers.
     */
    private void registerDefaultHandlers() {
        registerHandler(new LongTypeHandler());
        registerHandler(new IntTypeHandler());
        registerHandler(new StringTypeHandler());
        registerHandler(new BooleanTypeHandler());
        registerHandler(new DoubleTypeHandler());
        registerHandler(new ByteTypeHandler());
        registerHandler(new ShortTypeHandler());
        registerHandler(new CharTypeHandler());
        registerHandler(new FloatTypeHandler());
    }
    
    /**
     * Register a type handler.
     * 
     * @param <T> the type the handler supports
     * @param handler the handler to register
     */
    public <T> void registerHandler(TypeHandler<T> handler) {
        handlersByTypeCode.put(handler.getTypeCode(), handler);
        handlersByJavaType.put(handler.getJavaType(), handler);
    }
    
    /**
     * Get a handler by type code.
     * 
     * @param typeCode the type code (from {@link io.memris.spring.TypeCodes})
     * @return the handler, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> TypeHandler<T> getHandler(byte typeCode) {
        return (TypeHandler<T>) handlersByTypeCode.get(typeCode);
    }
    
    /**
     * Get a handler by Java type.
     * 
     * @param javaType the Java class
     * @return the handler, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> TypeHandler<T> getHandler(Class<T> javaType) {
        return (TypeHandler<T>) handlersByJavaType.get(javaType);
    }
    
    /**
     * Check if a handler is registered for the given type code.
     */
    public boolean hasHandler(byte typeCode) {
        return handlersByTypeCode.containsKey(typeCode);
    }
    
    /**
     * Check if a handler is registered for the given Java type.
     */
    public boolean hasHandler(Class<?> javaType) {
        return handlersByJavaType.containsKey(javaType);
    }
    
    /**
     * Remove a handler by type code.
     * 
     * @param typeCode the type code
     * @return the removed handler, or null if not found
     */
    @SuppressWarnings("unchecked")
    public <T> TypeHandler<T> removeHandler(byte typeCode) {
        TypeHandler<?> handler = handlersByTypeCode.remove(typeCode);
        if (handler != null) {
            handlersByJavaType.remove(handler.getJavaType());
        }
        return (TypeHandler<T>) handler;
    }
    
    /**
     * Get all registered type codes.
     */
    public java.util.Set<Byte> getRegisteredTypeCodes() {
        return new java.util.HashSet<>(handlersByTypeCode.keySet());
    }
    
    /**
     * Get all registered Java types.
     */
    public java.util.Set<Class<?>> getRegisteredJavaTypes() {
        return new java.util.HashSet<>(handlersByJavaType.keySet());
    }
    
    /**
     * Clear all registered handlers.
     */
    public void clear() {
        handlersByTypeCode.clear();
        handlersByJavaType.clear();
    }
    
    /**
     * Holder for the default singleton instance.
     */
    private static class DefaultHolder {
        static final TypeHandlerRegistry INSTANCE = new TypeHandlerRegistry();
    }
}
