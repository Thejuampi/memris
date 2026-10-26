package io.memris.spring.data.repository.config;

import io.memris.spring.data.repository.support.MemrisRepositoryFactoryBean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Import;
import org.springframework.data.repository.config.DefaultRepositoryBaseClass;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Enables Memris Spring Data repositories for the annotated configuration class.
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(MemrisRepositoriesRegistrar.class)
public @interface EnableMemrisRepositories {
    /**
     * Additional base packages, using the standard Spring Data shorthand.
     *
     * @return package names to scan
     */
    String[] value() default {};

    /**
     * Filters that explicitly include repository candidates.
     *
     * @return inclusion filters
     */
    ComponentScan.Filter[] includeFilters() default {};

    /**
     * Filters that exclude repository candidates.
     *
     * @return exclusion filters
     */
    ComponentScan.Filter[] excludeFilters() default {};

    /**
     * Optional resource containing named queries.
     *
     * @return named-query resource location
     */
    String namedQueriesLocation() default "";

    /**
     * Suffix used when discovering custom repository implementations.
     *
     * @return implementation suffix
     */
    String repositoryImplementationPostfix() default "Impl";

    /**
     * Base packages to scan for repository interfaces.
     *
     * @return package names to scan
     */
    String[] basePackages() default {};

    /**
     * Marker classes whose packages will be scanned for repositories.
     *
     * @return marker classes
     */
    Class<?>[] basePackageClasses() default {};

    /**
     * Factory bean used to create repository proxies.
     *
     * @return repository factory bean class
     */
    Class<?> repositoryFactoryBeanClass() default MemrisRepositoryFactoryBean.class;

    /**
     * Optional repository implementation class; the default uses the generated Memris implementation.
     *
     * @return base repository implementation type
     */
    Class<?> repositoryBaseClass() default DefaultRepositoryBaseClass.class;

    /**
     * Whether nested repository interfaces should be considered.
     *
     * @return {@code true} to include nested repository interfaces
     */
    boolean considerNestedRepositories() default false;
}
