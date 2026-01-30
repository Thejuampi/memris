package io.memris.spring.runtime;

import io.memris.spring.EntityMetadata;
import io.memris.spring.EntityMetadata.FieldMapping;
import io.memris.spring.TypeCodes;
import io.memris.spring.converter.TypeConverter;
import net.bytebuddy.ByteBuddy;
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

public final class EntityMaterializerGenerator {

    public <T> EntityMaterializer<T> generate(EntityMetadata<T> metadata) {
        Class<T> entityClass = metadata.entityClass();
        Constructor<T> ctor = metadata.entityConstructor();
        if (ctor == null || !Modifier.isPublic(ctor.getModifiers())) {
            throw new IllegalStateException("Entity requires public no-arg constructor for direct materialization: " + entityClass.getName());
        }

        List<FieldInfo> fields = resolveFields(metadata);
        List<FieldInfo> converterFields = fields.stream().filter(f -> f.converter != null).toList();

        String implName = entityClass.getName() + "$MemrisMaterializer$" + System.nanoTime();
        DynamicType.Builder<?> builder = new ByteBuddy()
            .subclass(Object.class)
            .implement(EntityMaterializer.class)
            .name(implName)
            .modifiers(Visibility.PUBLIC, TypeManifestation.FINAL);

        for (int i = 0; i < converterFields.size(); i++) {
            builder = builder.defineField(converterFieldName(i), TypeConverter.class, Visibility.PRIVATE, FieldManifestation.FINAL);
        }

        if (!converterFields.isEmpty()) {
            Class<?>[] paramTypes = new Class<?>[converterFields.size()];
            for (int i = 0; i < converterFields.size(); i++) {
                paramTypes[i] = TypeConverter.class;
                converterFields.get(i).converterIndex = i;
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

            builder = builder.defineConstructor(Visibility.PUBLIC)
                .withParameters(paramTypes)
                .intercept(ctorCall);
        }

        builder = builder.defineMethod("materialize", entityClass, Visibility.PUBLIC)
            .withParameters(HeapRuntimeKernel.class, int.class)
            .intercept(new Implementation.Simple(new MaterializeAppender(entityClass, fields, converterFields)));

        try (DynamicType.Unloaded<?> unloaded = builder.make()) {
            Class<?> implClass = unloaded.load(entityClass.getClassLoader()).getLoaded();
            if (converterFields.isEmpty()) {
                return (EntityMaterializer<T>) implClass.getDeclaredConstructor().newInstance();
            }
            Object[] args = new Object[converterFields.size()];
            Class<?>[] paramTypes = new Class<?>[converterFields.size()];
            for (int i = 0; i < converterFields.size(); i++) {
                args[i] = converterFields.get(i).converter;
                paramTypes[i] = TypeConverter.class;
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
            try {
                Field field = entityClass.getDeclaredField(mapping.name());
                if (!Modifier.isPublic(field.getModifiers())) {
                    continue;
                }
                TypeConverter<?, ?> converter = metadata.converters().get(mapping.name());
                result.add(new FieldInfo(mapping, field, converter));
            } catch (NoSuchFieldException ignored) {
                // Skip fields not present
            }
        }
        return result;
    }

    private static String converterFieldName(int index) {
        return "c" + index;
    }

    private static final class FieldInfo {
        final FieldMapping mapping;
        final Field field;
        final TypeConverter<?, ?> converter;
        int converterIndex = -1;

        FieldInfo(FieldMapping mapping, Field field, TypeConverter<?, ?> converter) {
            this.mapping = mapping;
            this.field = field;
            this.converter = converter;
        }
    }

    private static final class MaterializeAppender implements ByteCodeAppender {
        private final Class<?> entityClass;
        private final List<FieldInfo> fields;
        private final List<FieldInfo> converterFields;

        private MaterializeAppender(Class<?> entityClass, List<FieldInfo> fields, List<FieldInfo> converterFields) {
            this.entityClass = entityClass;
            this.fields = fields;
            this.converterFields = converterFields;
        }

        @Override
        public Size apply(MethodVisitor mv, Implementation.Context context, net.bytebuddy.description.method.MethodDescription method) {
            String entityInternal = Type.getInternalName(entityClass);
            String tableInternal = Type.getInternalName(io.memris.storage.GeneratedTable.class);
            String kernelInternal = Type.getInternalName(HeapRuntimeKernel.class);
            String converterInternal = Type.getInternalName(TypeConverter.class);

            int entityVar = 3;
            int tableVar = 4;
            int intVar = 5;
            int longVar = 6;
            int doubleVar = 8;
            int floatVar = 10;
            int objVar = 11;

            mv.visitTypeInsn(Opcodes.NEW, entityInternal);
            mv.visitInsn(Opcodes.DUP);
            mv.visitMethodInsn(Opcodes.INVOKESPECIAL, entityInternal, "<init>", "()V", false);
            mv.visitVarInsn(Opcodes.ASTORE, entityVar);

            mv.visitVarInsn(Opcodes.ALOAD, 1);
            mv.visitMethodInsn(Opcodes.INVOKEVIRTUAL, kernelInternal, "table", "()Lio/memris/storage/GeneratedTable;", false);
            mv.visitVarInsn(Opcodes.ASTORE, tableVar);

            for (FieldInfo info : fields) {
                emitFieldWrite(mv, info, entityInternal, tableInternal, converterInternal, entityVar, tableVar,
                    intVar, longVar, doubleVar, floatVar, objVar);
            }

            mv.visitVarInsn(Opcodes.ALOAD, entityVar);
            mv.visitInsn(Opcodes.ARETURN);

            return new Size(8, objVar + 1);
        }

        private void emitFieldWrite(MethodVisitor mv,
                                    FieldInfo info,
                                    String entityInternal,
                                    String tableInternal,
                                    String converterInternal,
                                    int entityVar,
                                    int tableVar,
                                    int intVar,
                                    int longVar,
                                    int doubleVar,
                                    int floatVar,
                                    int objVar) {
            FieldMapping mapping = info.mapping;
            Field field = info.field;
            Class<?> fieldType = field.getType();
            int colIdx = mapping.columnPosition();

            if (info.converter != null) {
                emitStorageAsObject(mv, mapping.typeCode(), tableInternal, tableVar, colIdx, objVar, intVar, longVar, doubleVar, floatVar);

                mv.visitVarInsn(Opcodes.ALOAD, 0);
                mv.visitFieldInsn(Opcodes.GETFIELD, entityInternal, converterFieldName(info.converterIndex), "Lio/memris/spring/converter/TypeConverter;");
                mv.visitVarInsn(Opcodes.ALOAD, objVar);
                mv.visitMethodInsn(Opcodes.INVOKESTATIC,
                    Type.getInternalName(EntityMaterializerGenerator.class),
                    "convertOrNull",
                    "(Lio/memris/spring/converter/TypeConverter;Ljava/lang/Object;)Ljava/lang/Object;",
                    false);
                mv.visitVarInsn(Opcodes.ASTORE, objVar);

                mv.visitVarInsn(Opcodes.ALOAD, entityVar);
                emitCastOrUnbox(mv, fieldType, objVar);
                mv.visitFieldInsn(Opcodes.PUTFIELD, entityInternal, field.getName(), Type.getDescriptor(fieldType));
                return;
            }

            if (!fieldType.isPrimitive() && mapping.typeCode() != TypeCodes.TYPE_STRING) {
                emitReadBoxed(mv, mapping.typeCode(), tableInternal, tableVar, colIdx, objVar, intVar, longVar, doubleVar, floatVar);
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

        private void emitReadBoxed(MethodVisitor mv, byte typeCode, String tableInternal, int tableVar, int colIdx,
                                   int objVar, int intVar, int longVar, int doubleVar, int floatVar) {
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

        private void emitStorageToLocal(MethodVisitor mv, byte typeCode, String tableInternal, int tableVar, int colIdx, Class<?> fieldType,
                                        int intVar, int longVar, int doubleVar, int floatVar, int objVar) {
            switch (typeCode) {
                case TypeCodes.TYPE_LONG -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.LLOAD, longVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_INT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.ILOAD, intVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false);
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
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
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
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Byte", "valueOf", "(B)Ljava/lang/Byte;", false);
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
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false);
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
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_FLOAT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "intBitsToFloat", "(I)F", false);
                    mv.visitVarInsn(Opcodes.FSTORE, floatVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.FLOAD, floatVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false);
                        mv.visitVarInsn(Opcodes.ASTORE, objVar);
                    }
                }
                case TypeCodes.TYPE_DOUBLE -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    mv.visitVarInsn(Opcodes.LLOAD, longVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "longBitsToDouble", "(J)D", false);
                    mv.visitVarInsn(Opcodes.DSTORE, doubleVar);
                    if (!fieldType.isPrimitive()) {
                        mv.visitVarInsn(Opcodes.DLOAD, doubleVar);
                        mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false);
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

        private void emitStorageAsObject(MethodVisitor mv, byte typeCode, String tableInternal, int tableVar, int colIdx,
                                         int objVar, int intVar, int longVar, int doubleVar, int floatVar) {
            switch (typeCode) {
                case TypeCodes.TYPE_LONG -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    mv.visitVarInsn(Opcodes.LLOAD, longVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Long", "valueOf", "(J)Ljava/lang/Long;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_INT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Integer", "valueOf", "(I)Ljava/lang/Integer;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_STRING -> {
                    emitReadString(mv, tableInternal, tableVar, colIdx);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_BOOLEAN -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Boolean", "valueOf", "(Z)Ljava/lang/Boolean;", false);
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
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Short", "valueOf", "(S)Ljava/lang/Short;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_CHAR -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitInsn(Opcodes.I2C);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Character", "valueOf", "(C)Ljava/lang/Character;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_FLOAT -> {
                    emitReadInt(mv, tableInternal, tableVar, colIdx, intVar);
                    mv.visitVarInsn(Opcodes.ILOAD, intVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "intBitsToFloat", "(I)F", false);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Float", "valueOf", "(F)Ljava/lang/Float;", false);
                    mv.visitVarInsn(Opcodes.ASTORE, objVar);
                }
                case TypeCodes.TYPE_DOUBLE -> {
                    emitReadLong(mv, tableInternal, tableVar, colIdx, longVar);
                    mv.visitVarInsn(Opcodes.LLOAD, longVar);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "longBitsToDouble", "(J)D", false);
                    mv.visitMethodInsn(Opcodes.INVOKESTATIC, "java/lang/Double", "valueOf", "(D)Ljava/lang/Double;", false);
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
        return Float.valueOf(Float.intBitsToFloat(table.readInt(columnIndex, rowIndex)));
    }

    public static Object readBoxedDouble(io.memris.storage.GeneratedTable table, int columnIndex, int rowIndex) {
        if (!table.isPresent(columnIndex, rowIndex)) {
            return null;
        }
        return Double.valueOf(Double.longBitsToDouble(table.readLong(columnIndex, rowIndex)));
    }
}
