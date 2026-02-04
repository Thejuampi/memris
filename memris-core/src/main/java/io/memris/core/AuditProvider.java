package io.memris.core;

/**
 * Supplies audit principals for @CreatedBy/@LastModifiedBy.
 */
@FunctionalInterface
public interface AuditProvider {
    Object currentUser();
}
