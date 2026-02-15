package io.memris.runtime;

import io.memris.core.ColumnAccessPlan;
import io.memris.core.FloatEncoding;
import io.memris.core.EntityMetadata;
import io.memris.core.EntityMetadata.FieldMapping;
import io.memris.core.TypeCodes;
import io.memris.core.converter.TypeConverter;
import io.memris.core.converter.TypeConverterRegistry;
import io.memris.storage.GeneratedTable;
import net.bytebuddy.ByteBuddy;
import net.bytebuddy.description.method.MethodDescription;
import net.bytebuddy.description.modifier.FieldManifestation;
import net.bytebuddy.description.modifier.TypeManifestation;
import net.bytebuddy.description.modifier.Visibility;
import net.bytebuddy.dynamic.DynamicType;
import net.bytebuddy.implementation.Implementation;
import net.bytebuddy.implementation.MethodCall;
import net.bytebuddy.implementation.bytecode.ByteCodeAppender;
import net.bytebuddy.implementation.FieldAccessor;
import net.bytebuddy.jar.asm.Label;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import net.bytebuddy.jar.asm.Type;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

public final class EntityMaterializerGenerator {
    private static final ConcurrentHashMap<ShapeKey, EntityMaterializer<?>> MATERIALIZER_CACHE = new ConcurrentHashMap<>();

    /**
     * Generates an entity materializer implementation for one entity metadata shape.
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * final class Customer$MemrisMaterializer$123 implements EntityMaterializer<Customer> {
     *     public Customer materialize(GeneratedTable table, int rowIndex) {
     *         var entity = new Customer();
     *         // read typed column values, convert if configured, then assign fields
     *         return entity;
     *     }
     * }
     * }</pre>
     */
    public <T> EntityMaterializer<T> generate(EntityMetadata<T> metadata) {
        ShapeKey key = ShapeKey.from(metadata);
        @SuppressWarnings("unchecked")
        EntityMaterializer<T> cached = (EntityMaterializer<T>) MATERIALIZER_CACHE.get(key);
        if (cached != null) {
            return cached;
        }
        EntityMaterializer<T> generated = generateUncached(metadata);
        @SuppressWarnings("unchecked")
        EntityMaterializer<T> existing = (EntityMaterializer<T>) MATERIALIZER_CACHE.putIfAbsent(key, generated);
        return existing != null ? existing : generated;
    }

    static void clearCacheForTests() {
        MATERIALIZER_CACHE.clear();
    }

    private <T> EntityMaterializer<T> generateUncached(EntityMetadata<T> metadata) {
        Class<T> entityClass = metadata.entityClass();
        Constructor<T> ctor = metadata.entityConstructor();
        if (ctor == null || !Modifier.isPublic(ctor.getModifiers())) {
            throw new IllegalStateException(
                    "Entity requires public no-arg constructor for direct materialization: " + entityClass.getName());
        }

        List<FieldInfo> fields = resolveFields(metadata);
        List<FieldInfo> converterFields = fields.stream().filter(f -> f.converter != null).toList();
        List<FieldInfo> planFields = fields.stream().filter(f -> f.plan != null).toList();

        String implName = entityClass.getName() + "$MemrisMaterializer$" + System.nanoTime();
        DynamicType.Builder<?> builder = new ByteBuddy()
                .subclass(Object.class)
                .implement(EntityMaterializer.class)
                .name(implName)
                .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        for (int i = 0; i < converterFields.size(); i++) {
            builder = builder.defineField(converterFieldName(i), TypeConverter.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }
        for (int i = 0; i < planFields.size(); i++) {
            builder = builder.defineField(planFieldName(i), ColumnAccessPlan.class, Visibility.PRIVATE,
                    FieldManifestation.FINAL);
        }

        if (!converterFields.isEmpty() || !planFields.isEmpty()) {
            Class<?>[] paramTypes = new Class<?>[converterFields.size() + planFields.size()];
            for (int i = 0; i < converterFields.size(); i++) {
                paramTypes[i] = TypeConverter.class;
                converterFields.get(i).converterIndex = i;
            }
            for (int i = 0; i < planFields.size(); i++) {
                paramTypes[converterFields.size() + i] = ColumnAccessPlan.class;
                planFields.get(i).planIndex = i;
            }

            Implementation.Composable ctorCall;
            try {
                ctorCall = MethodCall.invoke(Object.class.getConstructor());
            } catch (NoSuchMethodException e) {
                throw new RuntimeException("Failed to resolve Object constructor", e);
            }
            for (int i = 0; i < converterFields.size(); i++) {
                ctorCall = ctorCall.andThen(FieldAccessor.ofField(converterFieldName(i)).setsArgumentAt(i));
            }
            for (int i = 0; i < planFields.size(); i++) {
                ctorCall = ctorCall.andThen(
                        FieldAccessor.ofField(planFieldName(i)).setsArgumentAt(converterFields.size() + i));
            }

            builder = builder.defineConstructor(Visibility.PUBLIC)
                    .withParameters(paramTypes)
                    .intercept(ctorCall);
        }

        builder = builder.defineMethod("materialize", entityClass, Visibility.PUBLIC)
                .withParameters(GeneratedTable.class, int.class)
                .intercept(new Implementation.Simple(new MaterializeAppender(entityClass, fields)));

        try (DynamicType.Unloaded<?> unloaded = builder.make()) {
            Class<?> implClass = unloaded.load(entityClass.getClassLoader()).getLoaded();
            if (converterFields.isEmpty() && planFields.isEmpty()) {
                return (EntityMaterializer<T>) implClass.getDeclaredConstructor().newInstance();
            }
            Object[] args = new Object[converterFields.size() + planFields.size()];
            Class<?>[] paramTypes = new Class<?>[converterFields.size() + planFields.size()];
            for (int i = 0; i < converterFields.size(); i++) {
                args[i] = converterFields.get(i).converter;
                paramTypes[i] = TypeConverter.class;
            }
            for (int i = 0; i < planFields.size(); i++) {
                args[converterFields.size() + i] = planFields.get(i).plan;
                paramTypes[converterFields.size() + i] = ColumnAccessPlan.class;
            }
            return (EntityMaterializer<T>) implClass.getDeclaredConstructor(paramTypes).newInstance(args);
        } catch (Exception e) {
            throw new RuntimeException("Failed to generate materializer for " + entityClass.getName(), e);
        }
    }

    private static List<FieldInfo> resolveFields(EntityMetadata<?> metadata) {
        Class<?> entityClass = metadata.entityClass();
        List<FieldInfo> result = new ArrayList<>();
        for (FieldMapping mapping : metadata.fields()) {
            if (mapping.isRelationship() || mapping.columnPosition() < 0) {
                continue;
            }
            Field directField = resolveDirectField(entityClass, mapping.name());
            ColumnAccessPlan plan = directField == null ? metadata.columnAccessPlan(mapping.name()) : null;
            TypeConverter<?, ?> converter = TypeConverterRegistry.getInstance()
                    .getFieldConverter(metadata.entityClass(), mapping.name());
            if (converter == null) {
                converter = metadata.converters().get(mapping.name());
            }
            result.add(new FieldInfo(mapping, directField, plan, converter));
        }
        return result;
    }

    private static Field resolveDirectField(Class<?> entityClass, String propertyName) {
        if (propertyName == null || propertyName.indexOf('.') >= 0) {
            return null;
        }
        try {
            Field field = entityClass.getDeclaredField(propertyName);
            return Modifier.isPublic(field.getModifiers()) ? field : null;
        } catch (NoSuchFieldException ignored) {
            return null;
        }
    }

    private static String converterFieldName(int index) {
        return "c" + index;
    }

    private static String planFieldName(int index) {
        return "p" + index;
    }

    private static final class FieldInfo {
        final FieldMapping mapping;
        final Field field;
        final ColumnAccessPlan plan;
        final TypeConverter<?, ?> converter;
        int converterIndex = -1;
        int planIndex = -1;

        FieldInfo(FieldMapping mapping, Field field, ColumnAccessPlan plan, TypeConverter<?, ?> converter) {
            this.mapping = mapping;
            this.field = field;
            this.plan = plan;
            this.converter = converter;
        }

        Class<?> targetType() {
            return field != null ? field.getType() : mapping.javaType();
        }
    }

    /**
     * ASM appender that emits the body of `materialize(table, rowIndex)`.
     *
     * Equivalent generated Java (simplified):
     *
     * <pre>{@code
     * Customer entity = new Customer();
     * entity.id = table.readLong(0, rowIndex);
     * entity.name = table.readString(1, rowIndex);
     * // converter fields are applied where configured
     * return entity;
     * }</pre>
     */
    private record MaterializeAppender(Class<?> entityClass, List<FieldInfo> fields) implements ByteCodeAppender {

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context, MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String materializerInternal = context.getInstrumentedType().getInternalName();
            String tableInternal = Type.getInternalName(GeneratedTable.class);
            int entityVar = 3;
            int tableVar = 1;
            int intVar = 5;
            int longVar = 6;
            int doubleVar = 8;
            int floatVar = 10;
            int objVar = 11;

            mv.visitTypeInsn(Opcodes.NEW, entityInternal);
            mv.visitInsn(Opcodes.DUP);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, entityInternal, "<init>", "()V", false);
            mv.visitVarInsn(Opcodes.ASTORE, entityVar);

            for (FieldInfo info : fields) {
                emitFieldWrite(mv, info, entityInternal, materializerInternal, tableInternal, entityVar, tableVar,
                        intVar, longVar, doubleVar, floatVar, objVar);
            }

            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitInsn(Opcodes.ARETURN);

            return new Size(8, objVar + 1);
        }

        private void emitFieldWrite(MethodVisitor mv,
                FieldInfo info,
                String entityInternal,
                String materializerInternal,
                String tableInternal,
                int entityVar,
                int tableVar,
                int intVar,
                int longVar,
                int doubleVar,
                int floatVar,
                int objVar) {
            FieldMapping mapping = info.mapping;
            Field field = info.field;
            Class<?> fieldType = info.targetType();
            int colIdx = mapping.columnPosition();

            if (info.plan != null) {
                emitStorageAsObjectNullable(mv, mapping.typeCode(), tableVar, colIdx, objVar);
                if (info.converter != null) {
                    mv.visitVarInsn(Opcodes.ALOAD, 0);
                    mv.visitFieldInsn(Opcodes.GETFIELD, materializerInternal, converterFieldName(info.converterIndex),
                            "Lio/memris/core/converter/TypeConverter;");
                    mv.visitVarInsn(Opcodes.ALOAD, objVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                            Type.getInternalName(EntityMaterializerGenerator.class),
                            "convertOrNull",
                            "(Lio/memris/core/converter/TypeConverter;Ljava/lang/Object;)Ljava/lang/Object;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, materializerInternal, planFieldName(info.planIndex),
                        "Lio/memris/core/ColumnAccessPlan;");
                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntityMaterializerGenerator.class),
                        "setWithPlanIfPresent",
                        "(Lio/memris/core/ColumnAccessPlan;Ljava/lang/Object;Ljava/lang/Object;)V",
                        false);
                return;
            }

            if (info.converter != null) {
                emitStorageAsObject(mv, mapping.typeCode(), tableInternal, tableVar, colIdx, objVar, intVar, longVar);

                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, materializerInternal, converterFieldName(info.converterIndex),
                        "Lio/memris/core/converter/TypeConverter;");
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                        Type.getInternalName(EntityMaterializerGenerator.class),
                        "convertOrNull",
                        "(Lio/memris/core/converter/TypeConverter;Ljava/lang/Object;)Ljava/lang/Object;",
                        false);
                mv.visitVarInsn(Opcodes.ASTORE, objVar);

                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                emitCastOrUnbox(mv, fieldType, objVar);
                mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));
                return;
            }

            if (!fieldType.isPrimitive() && mapping.typeCode() != TypeCodes.TYPE_STRING) {
                emitReadBoxed(mv, mapping.typeCode(), tableVar, colIdx, objVar);
                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                emitCastOrUnbox(mv, fieldType, objVar);
                mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));
                return;
            }

            emitStorageToLocal(mv, mapping.typeCode(), tableInternal, tableVar, colIdx, fieldType,
                    intVar, longVar, doubleVar, floatVar, objVar);
            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            emitLoadForField(mv, mapping.typeCode(), fieldType, intVar, longVar, doubleVar, floatVar, objVar);
            mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));
        }

        private void emitStorageAsObjectNullable(MethodVisitor mv, byte typeCode, int tableVar, int colIdx, int objVar) {
            String owner = Type.getInternalName(EntityMaterializerGenerator.class);
            String methodName = switch (typeCode) {
                case TypeCodes.TYPE_LONG,
                        TypeCodes.TYPE_INSTANT,
                        TypeCodes.TYPE_LOCAL_DATE,
                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                        TypeCodes.TYPE_DATE -> "readBoxedLong";
                case TypeCodes.TYPE_INT -> "readBoxedInt";
                case TypeCodes.TYPE_STRING,
                        TypeCodes.TYPE_BIG_DECIMAL,
                        TypeCodes.TYPE_BIG_INTEGER -> "readStringIfPresent";
                case TypeCodes.TYPE_BOOLEAN -> "readBoxedBoolean";
                case TypeCodes.TYPE_BYTE -> "readBoxedByte";
                case TypeCodes.TYPE_SHORT -> "readBoxedShort";
                case TypeCodes.TYPE_CHAR -> "readBoxedChar";
                case TypeCodes.TYPE_FLOAT -> "readBoxedFloat";
                case TypeCodes.TYPE_DOUBLE -> "readBoxedDouble";
                default -> throw new IllegalStateException("Unknown type code: " + typeCode);
            };
            mv.visitVarInsn(Opcodes.ALOAD, tableVar);
            pushInt(mv, colIdx);
            mv.visitVarInsn(Opcodes.ILOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    owner,
                    methodName,
                    "(Lio/memris/storage/GeneratedTable;II)Ljava/lang/Object;",
                    false);
            mv.visitVarInsn(Opcodes.ASTORE, objVar);
        }

        private void emitReadBoxed(MethodVisitor mv, byte typeCode, int tableVar, int colIdx, int objVar) {
            String owner = Type.getInternalName(EntityMaterializerGenerator.class);
            String methodName = switch (typeCode) {
                case TypeCodes.TYPE_LONG -> "readBoxedLong";
                case TypeCodes.TYPE_INT -> "readBoxedInt";
                case TypeCodes.TYPE_BOOLEAN -> "readBoxedBoolean";
                case TypeCodes.TYPE_BYTE -> "readBoxedByte";
                case TypeCodes.TYPE_SHORT -> "readBoxedShort";
                case TypeCodes.TYPE_CHAR -> "readBoxedChar";
                case TypeCodes.TYPE_FLOAT -> "readBoxedFloat";
                case TypeCodes.TYPE_DOUBLE -> "readBoxedDouble";
                default -> throw new IllegalStateException("Unsupported boxed type code: " + typeCode);
            };
            mv.visitVarInsn(Opcodes.ALOAD, tableVar);
            pushInt(mv, colIdx);
            mv.visitVarInsn(Opcodes.ILOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    owner,
                    methodName,
                    "(Lio/memris/storage/GeneratedTable;II)Ljava/lang/Object;",
                    false);
            mv.visitVarInsn(Opcodes.ASTORE, objVar);
        }

        private void emitStorageToLocal(MethodVisitor mv, byte typeCode, String tableInternal, int tableVar, int colIdx,
                Class<?> fieldType,
                int intVar, int longVar, int doubleVar, int floatVar, int objVar) {
            switch (typeCode) {
                case TypeCodes.TYPE_LONG -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.LLOAD, longVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;",
                                false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_INT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.ILOAD, intVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf",
                                "(I)Ljava/lang/Integer;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_STRING -> {
                    emitReadString(mv, tableInternal, tableVar, colIdx);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_BOOLEAN -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    Label isFalse = new Label();
                    Label end = new Label();
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitJumpInsn(Opcodes.IFEQ, isFalse);
                    mv.visitInsn(Opcodes.ICONST_1);
                    mv.visitJumpInsn(Opcodes.GOTO, end);
                    mv.visitLabel(isFalse);
                    mv.visitInsn(Opcodes.ICONST_0);
                    mv.visitLabel(end);
                    mv.visitVarInsn(Opcodes.ISTORE, intVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.ILOAD, intVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf",
                                "(Z)Ljava/lang/Boolean;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_BYTE -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2B);
                    mv.visitVarInsn(Opcodes.ISTORE, intVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.ILOAD, intVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;",
                                false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_SHORT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2S);
                    mv.visitVarInsn(Opcodes.ISTORE, intVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.ILOAD, intVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;",
                                false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_CHAR -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2C);
                    mv.visitVarInsn(Opcodes.ISTORE, intVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.ILOAD, intVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Character", "valueOf",
                                "(C)Ljava/lang/Character;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_FLOAT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "io/memris/core/FloatEncoding", "sortableIntToFloat",
                            "(I)F", false);
                    mv.visitVarInsn(Opcodes.FSTORE, floatVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.FLOAD, floatVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;",
                                false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_DOUBLE -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    mv.visitVarInsn(Opcodes.LLOAD, longVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "io/memris/core/FloatEncoding", "sortableLongToDouble",
                            "(J)D", false);
                    mv.visitVarInsn(Opcodes.DSTORE, doubleVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.DLOAD, doubleVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;",
                                false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                default -> throw new IllegalStateException("Unknown type code: " + typeCode);
            }
        }

        private void emitLoadForField(MethodVisitor mv, byte typeCode, Class<?> fieldType,
                int intVar, int longVar, int doubleVar, int floatVar, int objVar) {
            if (!fieldType.isPrimitive()) {
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                return;
            }
            switch (typeCode) {
                case TypeCodes.TYPE_LONG -> mv.visitVarInsn(Opcodes.LLOAD, longVar);
                case TypeCodes.TYPE_DOUBLE -> mv.visitVarInsn(Opcodes.DLOAD, doubleVar);
                case TypeCodes.TYPE_FLOAT -> mv.visitVarInsn(Opcodes.FLOAD, floatVar);
                default -> mv.visitVarInsn(Opcodes.ILOAD, intVar);
            }
        }

        private void emitStorageAsObject(MethodVisitor mv, byte typeCode, String tableInternal, int tableVar,
                int colIdx,
                int objVar, int intVar, int longVar) {
            switch (typeCode) {

                case TypeCodes.TYPE_DOUBLE -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    mv.visitVarInsn(Opcodes.LLOAD, longVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "io/memris/core/FloatEncoding", "sortableLongToDouble",
                            "(J)D", false);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_LONG,
                        TypeCodes.TYPE_INSTANT,
                        TypeCodes.TYPE_LOCAL_DATE,
                        TypeCodes.TYPE_LOCAL_DATE_TIME,
                        TypeCodes.TYPE_DATE -> {
                    mv.visitVarInsn(Opcodes.ALOAD, tableVar);
                    pushInt(mv, colIdx);
                    mv.visitVarInsn(Opcodes.ILOAD, 2);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                            Type.getInternalName(EntityMaterializerGenerator.class),
                            "readBoxedLong",
                            "(Lio/memris/storage/GeneratedTable;II)Ljava/lang/Object;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_INT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_STRING,
                        TypeCodes.TYPE_BIG_DECIMAL,
                        TypeCodes.TYPE_BIG_INTEGER -> {
                    emitReadString(mv, tableInternal, tableVar, colIdx);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_BOOLEAN -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_BYTE -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2B);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_SHORT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2S);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_CHAR -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2C);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Character", "valueOf",
                            "(C)Ljava/lang/Character;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_FLOAT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "io/memris/core/FloatEncoding", "sortableIntToFloat",
                            "(I)F", false);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;",
                            false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }

                default -> throw new IllegalStateException("Unknown type code: " + typeCode);
            }
        }

        private void emitCastOrUnbox(MethodVisitor mv, Class<?> fieldType, int valueVar) {
            if (!fieldType.isPrimitive()) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(fieldType));
                return;
            }
            if (fieldType == int.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Integer");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Integer", "intValue", "()I", false);
            } else if (fieldType == long.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Long");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Long", "longValue", "()J", false);
            } else if (fieldType == boolean.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Boolean");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Boolean", "booleanValue", "()Z", false);
            } else if (fieldType == byte.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Byte");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Byte", "byteValue", "()B", false);
            } else if (fieldType == short.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Short");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Short", "shortValue", "()S", false);
            } else if (fieldType == char.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Character");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Character", "charValue", "()C", false);
            } else if (fieldType == float.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Float");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Float", "floatValue", "()F", false);
            } else if (fieldType == double.class) {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, "java/lang/Double");
                mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, "java/lang/Double", "doubleValue", "()D", false);
            } else {
                mv.visitVarInsn(Opcodes.ALOAD, valueVar);
                mv.visitTypeInsn(Opcodes.CHECKCAST, Type.getInternalName(fieldType));
            }
        }

        private void emitReadInt(MethodVisitor mv, String tableInternal, int tableVar, int colIdx, int valueVar) {
            mv.visitVarInsn(Opcodes.ALOAD, tableVar);
            pushInt(mv, colIdx);
            mv.visitVarInsn(Opcodes.ILOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, tableInternal, "readInt", "(II)I", true);
            mv.visitVarInsn(Opcodes.ISTORE, valueVar);
        }

        private void emitReadLong(MethodVisitor mv, String tableInternal, int tableVar, int colIdx, int valueVar) {
            mv.visitVarInsn(Opcodes.ALOAD, tableVar);
            pushInt(mv, colIdx);
            mv.visitVarInsn(Opcodes.ILOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, tableInternal, "readLong", "(II)J", true);
            mv.visitVarInsn(Opcodes.LSTORE, valueVar);
        }

        private void emitReadString(MethodVisitor mv, String tableInternal, int tableVar, int colIdx) {
            mv.visitVarInsn(Opcodes.ALOAD, tableVar);
            pushInt(mv, colIdx);
            mv.visitVarInsn(Opcodes.ILOAD, 2);
            mv.visitMethodInsn(Opcodes.INVOKEINTERFACE, tableInternal, "readString", "(II)Ljava/lang/String;", true);
        }

        private void pushInt(MethodVisitor mv, int value) {
            if (value == -1) {
                mv.visitInsn(Opcodes.ICONST_M1);
            } else if (value >= 0 && value <= 5) {
                mv.visitInsn(Opcodes.ICONST_0 + value);
            } else if (value >= Byte.MIN_VALUE && value <= Byte.MAX_VALUE) {
                mv.visitIntInsn(Opcodes.BIPUSH, value);
            } else if (value >= Short.MIN_VALUE && value <= Short.MAX_VALUE) {
                mv.visitIntInsn(Opcodes.SIPUSH, value);
            } else {
                mv.visitLdcInsn(value);
            }
        }
    }

    public static Object convertOrNull(TypeConverter<?, ?> converter, Object value) {
        if (value == null) {
            return null;
        }
        @SuppressWarnings("unchecked")
        TypeConverter<Object, Object> typed = (TypeConverter<Object, Object>) converter;
        return typed.fromStorage(value);
    }

    public static void setWithPlanIfPresent(ColumnAccessPlan plan, Object root, Object value) {
        if (value == null) {
            return;
        }
        if (plan == null || root == null) {
            return;
        }
        plan.set(root, value);
    }

    public static Object readBoxedLong(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Long.valueOf(table.readLong(columnIndex, rowIndex));
    }

    public static Object readBoxedInt(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Integer.valueOf(table.readInt(columnIndex, rowIndex));
    }

    public static Object readBoxedBoolean(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Boolean.valueOf(table.readInt(columnIndex, rowIndex) != 0);
    }

    public static Object readBoxedByte(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Byte.valueOf((byte) table.readInt(columnIndex, rowIndex));
    }

    public static Object readBoxedShort(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Short.valueOf((short) table.readInt(columnIndex, rowIndex));
    }

    public static Object readBoxedChar(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Character.valueOf((char) table.readInt(columnIndex, rowIndex));
    }

    public static Object readBoxedFloat(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Float.valueOf(FloatEncoding.sortableIntToFloat(table.readInt(columnIndex, rowIndex)));
    }

    public static Object readBoxedDouble(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Double.valueOf(FloatEncoding.sortableLongToDouble(table.readLong(columnIndex, rowIndex)));
    }

    public static Object readStringIfPresent(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return table.readString(columnIndex, rowIndex);
    }

    private static final class ShapeKey {
        private final ClassLoader classLoader;
        private final Class<?> entityClass;
        private final String columnSignature;
        private final String converterSignature;

        private ShapeKey(ClassLoader classLoader, Class<?> entityClass, String columnSignature,
                String converterSignature) {
            this.classLoader = classLoader;
            this.entityClass = entityClass;
            this.columnSignature = columnSignature;
            this.converterSignature = converterSignature;
        }

        static ShapeKey from(EntityMetadata<?> metadata) {
            var fields = metadata.fields().stream()
                    .filter(mapping -> mapping.columnPosition() >= 0 && !mapping.isRelationship())
                    .sorted(java.util.Comparator.comparingInt(FieldMapping::columnPosition))
                    .toList();
            var columnBuilder = new StringBuilder(fields.size() * 48);
            var converterBuilder = new StringBuilder(fields.size() * 32);
            for (var mapping : fields) {
                columnBuilder.append(mapping.columnPosition())
                        .append(':')
                        .append(mapping.name())
                        .append(':')
                        .append(mapping.typeCode())
                        .append(':')
                        .append(mapping.javaType().getName())
                        .append('|');
                var converter = resolveConverter(metadata, mapping.name());
                converterBuilder.append(mapping.columnPosition())
                        .append(':')
                        .append(System.identityHashCode(converter))
                        .append('|');
            }
            return new ShapeKey(
                    metadata.entityClass().getClassLoader(),
                    metadata.entityClass(),
                    columnBuilder.toString(),
                    converterBuilder.toString());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (!(o instanceof ShapeKey other)) {
                return false;
            }
            return classLoader == other.classLoader
                    && entityClass.equals(other.entityClass)
                    && columnSignature.equals(other.columnSignature)
                    && converterSignature.equals(other.converterSignature);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    System.identityHashCode(classLoader),
                    entityClass,
                    columnSignature,
                    converterSignature);
        }
    }

    private static TypeConverter<?, ?> resolveConverter(EntityMetadata<?> metadata, String propertyName) {
        var fieldConverter = TypeConverterRegistry.getInstance()
                .getFieldConverter(metadata.entityClass(), propertyName);
        if (fieldConverter != null) {
            return fieldConverter;
        }
        return metadata.converters().get(propertyName);
    }
}
