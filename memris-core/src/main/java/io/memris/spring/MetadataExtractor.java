package io.memris.spring;

import jakarta.persistence.Id;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.Constructor;
import java.util.*;

/**
 * Extracts metadata from entity classes for repository generation.
 * All reflection work happens once during metadata extraction.
 */
public final class MetadataExtractor {

    /**
     * Extract metadata from an entity class.
     * 
     * @param entityClass the entity class
     * @return the entity metadata
     */
    public static <T> EntityMetadata<T> extractEntityMetadata(Class<T> entityClass) {
        // TODO: Implement full metadata extraction
        // For now, return minimal metadata to allow compilation
        
        try {
            Constructor<T> constructor = entityClass.getDeclaredConstructor();
            
            // Find ID field
            String idColumnName = "id";
            for (var field : entityClass.getDeclaredFields()) {
                if (field.getName().equals("id") || 
                    field.isAnnotationPresent(Id.class) ||
                    field.isAnnotationPresent(GeneratedValue.class)) {
                    idColumnName = field.getName();
                    break;
                }
            }
            
            // Build field mappings
            List<EntityMetadata.FieldMapping> fields = new ArrayList<>();
            int colPos = 0;
            for (var field : entityClass.getDeclaredFields()) {
                if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                    continue;
                }
                
                Byte typeCode = TypeCodes.forClassOrDefault(field.getType(), TypeCodes.TYPE_LONG);
                fields.add(new EntityMetadata.FieldMapping(
                    field.getName(),
                    field.getName(),
                    field.getType(),
                    field.getType(),
                    colPos++,
                    typeCode
                ));
            }
            
            // Build MethodHandles for field access (for public fields)
            Map<String, MethodHandle> fieldGetters = new HashMap<>();
            Map<String, MethodHandle> fieldSetters = new HashMap<>();
            
            try {
                java.lang.invoke.MethodHandles.Lookup lookup = java.lang.invoke.MethodHandles.publicLookup();
                
                for (var field : entityClass.getDeclaredFields()) {
                    if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                        continue;
                    }
                    
                    // Create getter handle for public fields
                    if (java.lang.reflect.Modifier.isPublic(field.getModifiers())) {
                        MethodHandle getter = lookup.unreflectGetter(field);
                        fieldGetters.put(field.getName(), getter);
                        
                        MethodHandle setter = lookup.unreflectSetter(field);
                        fieldSetters.put(field.getName(), setter);
                    }
                }
            } catch (Exception e) {
                // If we can't create MethodHandles, continue with empty maps
                // (entity materialization will fall back to reflection)
            }
            
            return new EntityMetadata<>(
                entityClass,
                constructor,
                idColumnName,
                fields,
                Set.of(),
                Map.of(),
                null, null, null,
                fieldGetters, fieldSetters,
                entityClass.isRecord()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract metadata for " + entityClass.getName(), e);
        }
    }
}
