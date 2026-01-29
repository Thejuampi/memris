package io.memris.spring.scaffold;

import io.memris.spring.MemrisRepository;
import io.memris.spring.runtime.RepositoryRuntime;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.matcher.ElementMatchers;

import java.lang.reflect.Method;
import java.util.Arrays;

/**
 * Generates repository implementations using ByteBuddy.
 * <p>
 * Creates dynamic proxy classes that implement repository interfaces and
 * delegate method calls to the RepositoryRuntime.
 */
public final class RepositoryEmitter {

    private final ByteBuddy byteBuddy;

    public RepositoryEmitter() {
        this.byteBuddy = new ByteBuddy();
    }

    /**
     * Static factory method for creating repositories with arena scoping.
     * This is called by MemrisArena to create arena-scoped repositories.
     *
     * @param repositoryInterface the repository interface class
     * @param arena               the arena for scoping
     * @param <T>               the entity type
     * @param <R>               the repository interface type
     * @return an instance of the generated repository implementation
     */
    @SuppressWarnings("unchecked")
    public static <T, R extends MemrisRepository<T>> R createRepository(Class<R> repositoryInterface, 
                                                                        io.memris.spring.MemrisArena arena) {
        // For now, create a basic runtime and emitter
        // TODO: Build proper RepositoryPlan with arena-scoped table
        RepositoryEmitter emitter = new RepositoryEmitter();
        
        // Extract entity class and create a minimal runtime
        Class<T> entityClass = extractEntityClass(repositoryInterface);
        io.memris.storage.GeneratedTable table = arena.getOrCreateTable(entityClass);
        
        io.memris.spring.runtime.RepositoryPlan<T> plan = io.memris.spring.runtime.RepositoryPlan.<T>builder()
                .entityClass(entityClass)
                .table(table)
                .kernel(new io.memris.spring.runtime.HeapRuntimeKernel(table))
                .queries(new io.memris.spring.plan.CompiledQuery[0])
                .build();
        
        io.memris.spring.runtime.RepositoryRuntime<T> runtime = 
                new io.memris.spring.runtime.RepositoryRuntime<>(plan, null);
        
        return emitter.emitAndInstantiate(repositoryInterface, runtime);
    }
    
    @SuppressWarnings("unchecked")
    private static <T> Class<T> extractEntityClass(Class<? extends MemrisRepository<?>> repositoryInterface) {
        for (java.lang.reflect.Type iface : repositoryInterface.getGenericInterfaces()) {
            if (iface instanceof java.lang.reflect.ParameterizedType pt) {
                java.lang.reflect.Type[] typeArgs = pt.getActualTypeArguments();
                if (typeArgs.length > 0 && typeArgs[0] instanceof Class<?>) {
                    return (Class<T>) typeArgs[0];
                }
            }
        }
        throw new IllegalArgumentException("Cannot extract entity class from " + repositoryInterface);
    }

    /**
     * Emit and instantiate a repository implementation for the given interface.
     *
     * @param repositoryInterface the repository interface class
     * @param runtime             the repository runtime for method delegation
     * @param <T>               the entity type
     * @param <R>               the repository interface type
     * @return an instance of the generated repository implementation
     */
    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R emitAndInstantiate(Class<R> repositoryInterface, RepositoryRuntime<T> runtime) {
        try {
            // Generate the implementation class
            Class<? extends R> implClass = generateImplementation(repositoryInterface, runtime);
            
            // Instantiate using no-arg constructor
            return implClass.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException("Failed to emit and instantiate repository for " + repositoryInterface.getName(), e);
        }
    }

    /**
     * Generate the repository implementation class.
     */
    @SuppressWarnings("unchecked")
    private <T, R extends MemrisRepository<T>> Class<? extends R> generateImplementation(Class<R> repositoryInterface, RepositoryRuntime<T> runtime) {
        String implClassName = repositoryInterface.getName() + "_MemrisImpl_" + System.nanoTime();
        
        DynamicType.Builder<?> builder = byteBuddy
                .subclass(Object.class)
                .implement(repositoryInterface)
                .name(implClassName)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        // Intercept all methods from the repository interface
        for (Method method : repositoryInterface.getMethods()) {
            if (method.isDefault() || java.lang.reflect.Modifier.isStatic(method.getModifiers())) {
                continue; // Skip default and static methods
            }
            
            builder = builder.method(ElementMatchers.named(method.getName())
                    .and(ElementMatchers.takesArguments(method.getParameterCount())))
                    .intercept(MethodDelegation.to(new RepositoryMethodInterceptor<>(runtime, method)));
        }

        try (DynamicType.Unloaded<?> unloaded = builder.make()) {
            return (Class<? extends R>) unloaded.load(repositoryInterface.getClassLoader()).getLoaded();
        }
    }

    /**
     * Method interceptor that delegates calls to RepositoryRuntime.
     */
    public static class RepositoryMethodInterceptor<T> {
        private final RepositoryRuntime<T> runtime;
        private final Method method;

        public RepositoryMethodInterceptor(RepositoryRuntime<T> runtime, Method method) {
            this.runtime = runtime;
            this.method = method;
        }

        @net.bytebuddy.implementation.bind.annotation.RuntimeType
        public Object intercept(
                @net.bytebuddy.implementation.bind.annotation.AllArguments Object[] args) {
            // Delegate to runtime based on method name and arguments
            return runtime.executeMethod(method, args);
        }
    }
}
