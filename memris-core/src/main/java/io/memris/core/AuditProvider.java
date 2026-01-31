package io.memris.core;

/**
 * Supplies audit principals for @CreatedBy/@LastModifiedBy.
 */
public interface AuditProvider {
    Object currentUser();
}
