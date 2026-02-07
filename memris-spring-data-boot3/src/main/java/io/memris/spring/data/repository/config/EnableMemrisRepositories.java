package io.memris.spring.data.repository.config;

import io.memris.spring.data.repository.MemrisSpringRepository;
import io.memris.spring.data.repository.support.MemrisRepositoryFactoryBean;
import org.springframework.context.annotation.Import;

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
     * Base repository interface that discovered repositories should extend.
     *
     * @return base repository interface type
     */
    Class<?> repositoryBaseClass() default MemrisSpringRepository.class;

    /**
     * Whether nested repository interfaces should be considered.
     *
     * @return {@code true} to include nested repository interfaces
     */
    boolean considerNestedRepositories() default false;
}
