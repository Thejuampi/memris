package io.memris.spring.data.repository.config;

import org.springframework.context.annotation.Import;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
@Import(MemrisRepositoriesRegistrar.class)
public @interface EnableMemrisRepositories {
    String[] basePackages() default {};

    Class<?>[] basePackageClasses() default {};

    Class<?> repositoryFactoryBeanClass() default io.memris.spring.data.repository.support.MemrisRepositoryFactoryBean.class;

    Class<?> repositoryBaseClass() default io.memris.spring.data.repository.MemrisSpringRepository.class;

    boolean considerNestedRepositories() default false;
}
