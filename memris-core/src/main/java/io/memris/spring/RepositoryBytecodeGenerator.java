package io.memris.spring;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.AllArguments;
import net.bytebuddy.implementation.bind.annotation.This;
import net.bytebuddy.implementation.bind.annotation.Origin;
import net.bytebuddy.implementation.bind.annotation.RuntimeType;
import net.bytebuddy.matcher.ElementMatchers;

import java.util.List;

public class RepositoryBytecodeGenerator {

    private final ByteBuddy byteBuddy;
    private final MemrisRepositoryFactory factory;

    public RepositoryBytecodeGenerator(MemrisRepositoryFactory factory) {
        this.byteBuddy = new ByteBuddy();
        this.factory = factory;
    }

    @SuppressWarnings("unchecked")
    public <T, R extends MemrisRepository<T>> R generateRepository(
            Class<R> repositoryInterface,
            EntityMetadata<T> entityMetadata,
            io.memris.storage.ffm.FfmTable table,
            List<QueryMetadata> queryMethods) {

        String className = repositoryInterface.getName().replace('$', '_') + "ByteBuddyImpl";

        try {
            ClassLoader classLoader = repositoryInterface.getClassLoader();
            try {
                @SuppressWarnings("unchecked")
                Class<? extends R> existingClass = (Class<? extends R>) Class.forName(className, false, classLoader);
                return existingClass
                    .getDeclaredConstructor(io.memris.storage.ffm.FfmTable.class, MemrisRepositoryFactory.class, EntityMetadata.class)
                    .newInstance(table, factory, entityMetadata);
            } catch (ClassNotFoundException ignored) {
                // proceed with generation
            }

            DynamicType.Builder<R> builder = byteBuddy
                .subclass(repositoryInterface)
                .name(className)
                .defineField("table", io.memris.storage.ffm.FfmTable.class, Visibility.PRIVATE)
                .defineField("factory", MemrisRepositoryFactory.class, Visibility.PRIVATE)
                .defineField("entityMetadata", EntityMetadata.class, Visibility.PRIVATE)
                .defineConstructor(Visibility.PUBLIC)
                .withParameters(io.memris.storage.ffm.FfmTable.class, MemrisRepositoryFactory.class, EntityMetadata.class)
                .intercept(MethodCall.invoke(Object.class.getConstructor())
                    .andThen(FieldAccessor.ofField("table").setsArgumentAt(0))
                    .andThen(FieldAccessor.ofField("factory").setsArgumentAt(1))
                    .andThen(FieldAccessor.ofField("entityMetadata").setsArgumentAt(2)));

            java.lang.reflect.Method[] methods = repositoryInterface.getMethods();
            for (java.lang.reflect.Method method : methods) {
                if (method.getDeclaringClass() == Object.class) {
                    continue;
                }

                String methodName = method.getName();
                Class<?> returnType = method.getReturnType();
                Class<?>[] paramTypes = method.getParameterTypes();

                builder = builder.defineMethod(methodName, returnType, Visibility.PUBLIC)
                    .withParameters(paramTypes)
                    .intercept(MethodDelegation.to(QueryMethodInterceptor.class));
            }

            Class<? extends R> generatedClass = builder.make()
                .load(classLoader, ClassLoadingStrategy.Default.INJECTION)
                .getLoaded();

            return generatedClass
                .getDeclaredConstructor(io.memris.storage.ffm.FfmTable.class, MemrisRepositoryFactory.class, EntityMetadata.class)
                .newInstance(table, factory, entityMetadata);

        } catch (Throwable t) {
            throw new RuntimeException("Failed to generate repository: " + className, t);
        }
    }

    public static class QueryMethodInterceptor {
        private static Object nullReturn(Class<?> returnType) {
            return returnType == void.class ? null : null;
        }

        private static Object finalizeReturn(Object result, Object entity, Class<?> returnType, Class<?> entityClass) {
            if (returnType == void.class) {
                return null;
            }
            if (returnType.isAssignableFrom(entityClass)) {
                return entity;
            }
            return result;
        }

        private static Object finalizeFindReturn(Object result, Class<?> returnType, Class<?> entityClass) {
            if (returnType == void.class) {
                return null;
            }
            if (returnType.isAssignableFrom(entityClass) && result instanceof java.util.Optional<?> opt) {
                return opt.orElse(null);
            }
            return result;
        }

        @RuntimeType
        public static Object intercept(
                @This Object instance,
                @AllArguments Object[] args,
                @Origin java.lang.reflect.Method method) {
            try {
                String methodName = method.getName();
                Class<?> returnType = method.getReturnType();
                java.lang.reflect.Field tableField = instance.getClass().getDeclaredField("table");
                tableField.setAccessible(true);
                io.memris.storage.ffm.FfmTable table = (io.memris.storage.ffm.FfmTable) tableField.get(instance);

                java.lang.reflect.Field factoryField = instance.getClass().getDeclaredField("factory");
                factoryField.setAccessible(true);
                MemrisRepositoryFactory factory = (MemrisRepositoryFactory) factoryField.get(instance);

                java.lang.reflect.Field metadataField = instance.getClass().getDeclaredField("entityMetadata");
                metadataField.setAccessible(true);
                EntityMetadata<?> metadata = (EntityMetadata<?>) metadataField.get(instance);

                Class<?> entityClass = metadata.entityClass();
                String idColumnName = metadata.idColumnName();

                return switch (methodName) {
                    case "save" -> finalizeReturn(factory.doSave(args[0], entityClass), args[0], returnType, entityClass);
                    case "saveAll" -> finalizeReturn(factory.doSaveAll(entityClass, (Iterable<?>) args[0]), null, returnType, entityClass);
                    case "findById" -> finalizeFindReturn(factory.doFindById(entityClass, idColumnName, args[0]), returnType, entityClass);
                    case "existsById" -> factory.doExistsById(entityClass, idColumnName, args[0]);
                    case "findAll" -> factory.doFindAll(entityClass);
                    case "findAllById" -> factory.doFindAllById(entityClass, (Iterable<?>) args[0]);
                    case "count" -> factory.doCount(entityClass);
                    case "deleteById" -> {
                        factory.doDeleteById(entityClass, idColumnName, args[0]);
                        yield nullReturn(returnType);
                    }
                    case "delete" -> {
                        factory.doDelete(entityClass, args[0]);
                        yield nullReturn(returnType);
                    }
                    case "deleteAllById" -> {
                        factory.doDeleteAllById(entityClass, (Iterable<?>) args[0]);
                        yield nullReturn(returnType);
                    }
                    case "deleteAll" -> {
                        if (args.length == 0) {
                            factory.doDeleteAll(entityClass);
                        } else {
                            factory.doDeleteAll(entityClass, (Iterable<?>) args[0]);
                        }
                        yield nullReturn(returnType);
                    }
                    case "update" -> {
                        factory.doUpdate(args[0], entityClass);
                        yield nullReturn(returnType);
                    }
                    default -> {
                        if (methodName.startsWith("findBy") || methodName.startsWith("countBy")) {
                            yield factory.doFindBy(entityClass, methodName, args, returnType);
                        }
                        if (methodName.equals("findBy")) {
                            yield factory.doGenericFindBy(entityClass, (String) args[0], args[1], returnType);
                        }
                        if (methodName.equals("findByIn")) {
                            yield factory.doFindByIn(entityClass, (String) args[0], (Iterable<?>) args[1], returnType);
                        }
                        if (methodName.equals("findByBetween")) {
                            yield factory.doFindByBetween(entityClass, (String) args[0], args[1], args[2], returnType);
                        }
                        throw new UnsupportedOperationException("Method not supported: " + methodName);
                    }
                };
            } catch (Throwable t) {
                throw new RuntimeException("Repository method failed", t);
            }
        }
    }

    static class FactoryHelper {
        static Object materialize(io.memris.storage.ffm.FfmTable table, Class<?> entityClass, int row) throws Exception {
            Object entity = entityClass.getDeclaredConstructor().newInstance();

            for (java.lang.reflect.Field field : entityClass.getDeclaredFields()) {
                field.setAccessible(true);
                String columnName = field.getName();

                if (columnName.endsWith("_id")) {
                    continue;
                }

                Object value = getFromTable(table, columnName, row, field.getType());
                field.set(entity, value);
            }

            return entity;
        }

        static java.util.List<Object> findAll(io.memris.storage.ffm.FfmTable table, Class<?> entityClass) throws Exception {
            io.memris.kernel.selection.SelectionVectorFactory factory =
                io.memris.kernel.selection.SelectionVectorFactory.defaultFactory();
            int[] rows = table.scanAll(factory).toIntArray();

            java.util.List<Object> results = new java.util.ArrayList<>(rows.length);
            for (int row : rows) {
                results.add(materialize(table, entityClass, row));
            }
            return results;
        }

        static Object genericFindBy(MemrisRepositoryFactory factory, io.memris.storage.ffm.FfmTable table,
                Class<?> entityClass, String columnName, Object value) throws Throwable {
            int[] matchingRows = factory.queryIndex(entityClass, columnName, io.memris.kernel.Predicate.Operator.EQ, value);

            if (matchingRows == null) {
                io.memris.kernel.Predicate predicate = new io.memris.kernel.Predicate.Comparison(columnName, io.memris.kernel.Predicate.Operator.EQ, value);
                io.memris.kernel.selection.SelectionVectorFactory svf =
                    io.memris.kernel.selection.SelectionVectorFactory.defaultFactory();
                matchingRows = table.scan(predicate, svf).toIntArray();
            }

            if (matchingRows == null || matchingRows.length == 0) {
                return java.util.Optional.empty();
            }

            java.util.List<Object> results = new java.util.ArrayList<>();
            for (int row : matchingRows) {
                results.add(materialize(table, entityClass, row));
            }

            return results;
        }

        static Object findByIn(MemrisRepositoryFactory factory, io.memris.storage.ffm.FfmTable table,
                Class<?> entityClass, String columnName, Iterable<?> values) throws Throwable {
            java.util.List<Object> results = new java.util.ArrayList<>();
            for (Object value : values) {
                int[] matchingRows = factory.queryIndex(entityClass, columnName, io.memris.kernel.Predicate.Operator.EQ, value);

                if (matchingRows != null) {
                    for (int row : matchingRows) {
                        results.add(materialize(table, entityClass, row));
                    }
                }
            }
            return results;
        }

        static Object findByBetween(MemrisRepositoryFactory factory, io.memris.storage.ffm.FfmTable table,
                Class<?> entityClass, String columnName, Object start, Object end) throws Throwable {
            io.memris.kernel.Predicate predicate = new io.memris.kernel.Predicate.Between(columnName, start, end);
            io.memris.kernel.selection.SelectionVectorFactory svf =
                io.memris.kernel.selection.SelectionVectorFactory.defaultFactory();
            int[] matchingRows = table.scan(predicate, svf).toIntArray();

            java.util.List<Object> results = new java.util.ArrayList<>();
            for (int row : matchingRows) {
                results.add(materialize(table, entityClass, row));
            }

            return results;
        }

        static Object findBy(MemrisRepositoryFactory factory, io.memris.storage.ffm.FfmTable table,
                Class<?> entityClass, String methodName, Object[] args) throws Throwable {

            String columnName = null;
            io.memris.kernel.Predicate.Operator operator = io.memris.kernel.Predicate.Operator.EQ;

            if (methodName.startsWith("findBy")) {
                String condition = methodName.substring(6);
                columnName = parseColumnName(condition);
                operator = parseOperator(condition);
            } else if (methodName.startsWith("countBy")) {
                String condition = methodName.substring(8);
                columnName = parseColumnName(condition);
                operator = io.memris.kernel.Predicate.Operator.EQ;
            } else {
                throw new UnsupportedOperationException("Method not supported: " + methodName);
            }

            if (args.length == 0) {
                if (methodName.startsWith("countBy")) {
                    return 0L;
                }
                return null;
            }

            Object paramValue = args[0];
            int[] matchingRows = factory.queryIndex(entityClass, columnName, operator, paramValue);

            if (matchingRows == null) {
                matchingRows = scanTable(table, columnName, operator, paramValue);
            }

            if (matchingRows == null || matchingRows.length == 0) {
                if (methodName.startsWith("findBy")) {
                    return java.util.Optional.empty();
                }
                if (methodName.startsWith("countBy")) {
                    return 0L;
                }
                return null;
            }

            java.util.List<Object> results = new java.util.ArrayList<>();
            for (int row : matchingRows) {
                results.add(materialize(table, entityClass, row));
            }

            if (methodName.startsWith("findBy")) {
                return java.util.Optional.of(results.get(0));
            }
            if (methodName.startsWith("countBy")) {
                return (long) results.size();
            }

            return null;
        }

        static Object save(io.memris.storage.ffm.FfmTable table, Class<?> entityClass, Object entity) throws Exception {
            String idColumnName = findIdColumnName(entityClass);
            Object idValue = null;

            for (java.lang.reflect.Field field : entityClass.getDeclaredFields()) {
                field.setAccessible(true);
                String columnName = field.getName();

                if (columnName.endsWith("_id")) {
                    continue;
                }

                Object value = field.get(entity);
                if (columnName.equals(idColumnName)) {
                    idValue = value;
                }
                setInTable(table, columnName, (int) table.rowCount(), value, field.getType());
            }

            return entity;
        }

        @SuppressWarnings("unchecked")
        static java.util.List<Object> saveAll(io.memris.storage.ffm.FfmTable table, Class<?> entityClass, Iterable<?> entities) throws Exception {
            java.util.List<Object> result = new java.util.ArrayList<>();
            for (Object entity : entities) {
                result.add(save(table, entityClass, entity));
            }
            return result;
        }

        static java.util.List<Object> findAllById(MemrisRepositoryFactory factory, io.memris.storage.ffm.FfmTable table,
                Class<?> entityClass, Iterable<?> ids) throws Exception {
            String idColumnName = findIdColumnName(entityClass);
            java.util.List<Object> results = new java.util.ArrayList<>();

            for (Object id : ids) {
                int[] matchingRows = factory.queryIndex(entityClass, idColumnName, io.memris.kernel.Predicate.Operator.EQ, id);
                if (matchingRows != null && matchingRows.length > 0) {
                    results.add(materialize(table, entityClass, matchingRows[0]));
                }
            }

            return results;
        }

        static void deleteById(MemrisRepositoryFactory factory, Class<?> entityClass,
                String idColumnName, Object id) throws Exception {
            int[] matchingRows = factory.queryIndex(entityClass, idColumnName, io.memris.kernel.Predicate.Operator.EQ, id);

            if (matchingRows == null || matchingRows.length == 0) {
                return;
            }

            Object index = factory.getIndex(entityClass, idColumnName);
            // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
            switch (index) {
                case io.memris.index.HashIndex<?> hashIndex -> {
                    @SuppressWarnings("unchecked")
                    io.memris.index.HashIndex<Object> rawIndex = (io.memris.index.HashIndex<Object>) hashIndex;
                    rawIndex.removeAll(id);
                }
                default -> {}
            }
        }

        static void deleteAllById(MemrisRepositoryFactory factory, Class<?> entityClass, Iterable<?> ids) throws Exception {
            String idColumnName = findIdColumnName(entityClass);
            for (Object id : ids) {
                deleteById(factory, entityClass, idColumnName, id);
            }
        }

        static void deleteAll(MemrisRepositoryFactory factory, Class<?> entityClass, Iterable<?> entities) throws Exception {
            String idColumnName = findIdColumnName(entityClass);
            for (Object entity : entities) {
                for (java.lang.reflect.Field field : entityClass.getDeclaredFields()) {
                    field.setAccessible(true);
                    if (field.getName().equals(idColumnName)) {
                        deleteById(factory, entityClass, idColumnName, field.get(entity));
                        break;
                    }
                }
            }
        }

        static void deleteAll(MemrisRepositoryFactory factory, Class<?> entityClass) throws Exception {
            String idColumnName = findIdColumnName(entityClass);
            if (idColumnName != null) {
                Object index = factory.getIndex(entityClass, idColumnName);
                // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
                switch (index) {
                    case io.memris.index.HashIndex<?> hashIndex -> {
                        @SuppressWarnings("unchecked")
                        io.memris.index.HashIndex<Object> rawIndex = (io.memris.index.HashIndex<Object>) hashIndex;
                        rawIndex.clear();
                    }
                    default -> {}
                }
            }
        }

        @SuppressWarnings("unchecked")
        static void setInTable(io.memris.storage.ffm.FfmTable table, String columnName,
                int row, Object value, Class<?> type) {
            io.memris.kernel.Column<?> column = table.column(columnName);
            // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
            try {
                switch (type) {
                    case Class<?> c when c == int.class || c == Integer.class ->
                        column.getClass().getMethod("set", int.class, Integer.class).invoke(column, row, value);
                    case Class<?> c when c == long.class || c == Long.class ->
                        column.getClass().getMethod("set", long.class, Long.class).invoke(column, row, value);
                    case Class<?> c when c == boolean.class || c == Boolean.class ->
                        column.getClass().getMethod("set", boolean.class, Boolean.class).invoke(column, row, value);
                    case Class<?> c when c == byte.class || c == Byte.class ->
                        column.getClass().getMethod("set", byte.class, Byte.class).invoke(column, row, value);
                    case Class<?> c when c == short.class || c == Short.class ->
                        column.getClass().getMethod("set", short.class, Short.class).invoke(column, row, value);
                    case Class<?> c when c == float.class || c == Float.class ->
                        column.getClass().getMethod("set", float.class, Float.class).invoke(column, row, value);
                    case Class<?> c when c == double.class || c == Double.class ->
                        column.getClass().getMethod("set", double.class, Double.class).invoke(column, row, value);
                    case Class<?> c when c == char.class || c == Character.class ->
                        column.getClass().getMethod("set", char.class, Character.class).invoke(column, row, value);
                    case Class<?> c when c == String.class ->
                        column.getClass().getMethod("set", int.class, String.class).invoke(column, row, value);
                    default -> throw new IllegalArgumentException("Unsupported type: " + type);
                }
            } catch (Exception e) {
                throw new RuntimeException("Failed to set column value", e);
            }
        }

        static String findIdColumnName(Class<?> entityClass) {
            for (java.lang.reflect.Field field : entityClass.getDeclaredFields()) {
                if (field.isAnnotationPresent(jakarta.persistence.GeneratedValue.class) ||
                    field.isAnnotationPresent(jakarta.persistence.Id.class) ||
                    field.getName().equals("id")) {
                    return field.getName();
                }
            }
            return null;
        }

        static String parseColumnName(String condition) {
            if (condition.contains("GreaterThan")) {
                return condition.substring(0, condition.indexOf("GreaterThan"));
            } else if (condition.contains("LessThan")) {
                return condition.substring(0, condition.indexOf("LessThan"));
            } else if (condition.contains("After")) {
                return condition.substring(0, condition.indexOf("After"));
            } else if (condition.contains("Before")) {
                return condition.substring(0, condition.indexOf("Before"));
            } else if (condition.contains("Containing")) {
                return condition.substring(0, condition.indexOf("Containing"));
            } else if (condition.contains("StartingWith")) {
                return condition.substring(0, condition.indexOf("StartingWith"));
            } else if (condition.contains("EndingWith")) {
                return condition.substring(0, condition.indexOf("EndingWith"));
            } else {
                return condition;
            }
        }

        static io.memris.kernel.Predicate.Operator parseOperator(String condition) {
            if (condition.contains("GreaterThan") || condition.contains("After")) {
                return io.memris.kernel.Predicate.Operator.GT;
            } else if (condition.contains("LessThan") || condition.contains("Before")) {
                return io.memris.kernel.Predicate.Operator.LT;
            } else {
                return io.memris.kernel.Predicate.Operator.EQ;
            }
        }

        static int[] scanTable(io.memris.storage.ffm.FfmTable table, String columnName,
                io.memris.kernel.Predicate.Operator operator, Object value) {
            io.memris.kernel.Predicate predicate = new io.memris.kernel.Predicate.Comparison(columnName, operator, value);
            io.memris.kernel.selection.SelectionVectorFactory factory =
                io.memris.kernel.selection.SelectionVectorFactory.defaultFactory();
            return table.scan(predicate, factory).toIntArray();
        }

        static Object getFromTable(io.memris.storage.ffm.FfmTable table, String columnName,
                int row, Class<?> type) {
            // Use pattern matching switch for O(1) dispatch instead of cascading if-else O(n)
            return switch (type) {
                case Class<?> c when c == int.class || c == Integer.class -> table.getInt(columnName, row);
                case Class<?> c when c == long.class || c == Long.class -> table.getLong(columnName, row);
                case Class<?> c when c == boolean.class || c == Boolean.class -> table.getBoolean(columnName, row);
                case Class<?> c when c == byte.class || c == Byte.class -> table.getByte(columnName, row);
                case Class<?> c when c == short.class || c == Short.class -> table.getShort(columnName, row);
                case Class<?> c when c == float.class || c == Float.class -> table.getFloat(columnName, row);
                case Class<?> c when c == double.class || c == Double.class -> table.getDouble(columnName, row);
                case Class<?> c when c == char.class || c == Character.class -> table.getChar(columnName, row);
                case Class<?> c when c == String.class -> table.getString(columnName, row);
                default -> throw new IllegalArgumentException("Unsupported type: " + type);
            };
        }
    }
}
