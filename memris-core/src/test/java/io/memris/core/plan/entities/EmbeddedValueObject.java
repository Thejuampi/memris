package io.memris.core.plan.entities;

import jakarta.persistence.*;

/**
 * Embeddable value object for embedded property tests.
 */
@Embeddable
public final class EmbeddedValueObject {
    private String firstName;
    private String lastName;
    private String email;

    public EmbeddedValueObject() {}

    public EmbeddedValueObject(String firstName, String lastName, String email) {
        this.firstName = firstName;
        this.lastName = lastName;
        this.email = email;
    }

    public String getFirstName() { return firstName; }
    public String getLastName() { return lastName; }
    public String getEmail() { return email; }
}
