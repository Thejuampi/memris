package io.memris.storage.heap;

import java.util.List;

/**
 * Metadata for table generation.
 */
public record TableMetadata(
        String entityName,
        String canonicalClassName,
        List<FieldMetadata> fields
) {
    public FieldMetadata idField() {
        return fields.stream()
                .filter(FieldMetadata::isId)
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException("No ID field found"));
    }

    public byte idTypeCode() {
        return idField().type();
    }
}
