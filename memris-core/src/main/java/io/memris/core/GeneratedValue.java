package io.memris.core;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Declares numeric ID generation for a Memris {@link Id} field.
 * The default engine supports Long and Integer IDs using a repository counter.
 * UUID, CUSTOM, and named generators are reserved and rejected during metadata initialization.
 */
@Target(ElementType.FIELD)
@Retention(RetentionPolicy.RUNTIME)
public @interface GeneratedValue {
    /**
     * ID generation strategy. AUTO and IDENTITY use the numeric repository counter.
     * UUID and CUSTOM are not implemented by the default engine.
     */
    GenerationType strategy() default GenerationType.AUTO;

    /**
     * Reserved custom generator name. Non-empty names are not supported by the default engine.
     */
    String generator() default "";
}
