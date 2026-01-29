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
            
            return new EntityMetadata<>(
                entityClass,
                constructor,
                idColumnName,
                fields,
                Set.of(),
                Map.of(),
                null, null, null,
                Map.of(), Map.of(),
                entityClass.isRecord()
            );
        } catch (Exception e) {
            throw new RuntimeException("Failed to extract metadata for " + entityClass.getName(), e);
        }
    }
}
