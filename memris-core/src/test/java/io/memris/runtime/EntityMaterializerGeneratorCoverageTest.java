package io.memris.runtime;

import io.memris.core.TypeCodes;
import net.bytebuddy.jar.asm.MethodVisitor;
import net.bytebuddy.jar.asm.Opcodes;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.List;

class EntityMaterializerGeneratorCoverageTest {

    @Test
    void shouldExerciseAppenderPrivateBranchesViaReflection() throws Exception {
        Class<?> appenderClass = Class.forName("io.memris.runtime.EntityMaterializerGenerator$MaterializeAppender");
        Constructor<?> ctor = appenderClass.getDeclaredConstructor(Class.class, List.class);
        ctor.setAccessible(true);
        Object appender = ctor.newInstance(EntityMaterializerGeneratorCoverageTest.class, List.of());

        Method emitReadBoxed = appenderClass.getDeclaredMethod(
                "emitReadBoxed", MethodVisitor.class, byte.class, int.class, int.class, int.class);
        Method emitStorageToLocal = appenderClass.getDeclaredMethod(
                "emitStorageToLocal", MethodVisitor.class, byte.class, String.class, int.class, int.class, Class.class,
                int.class, int.class, int.class, int.class, int.class);
        Method emitLoadForField = appenderClass.getDeclaredMethod(
                "emitLoadForField", MethodVisitor.class, byte.class, Class.class, int.class, int.class, int.class,
                int.class, int.class);
        Method pushInt = appenderClass.getDeclaredMethod("pushInt", MethodVisitor.class, int.class);
        emitReadBoxed.setAccessible(true);
        emitStorageToLocal.setAccessible(true);
        emitLoadForField.setAccessible(true);
        pushInt.setAccessible(true);

        MethodVisitor mv = new MethodVisitor(Opcodes.ASM9) {
        };

        byte[] boxedTypes = new byte[] {
                TypeCodes.TYPE_LONG,
                TypeCodes.TYPE_INT,
                TypeCodes.TYPE_BOOLEAN,
                TypeCodes.TYPE_BYTE,
                TypeCodes.TYPE_SHORT,
                TypeCodes.TYPE_CHAR,
                TypeCodes.TYPE_FLOAT,
                TypeCodes.TYPE_DOUBLE
        };
        for (byte type : boxedTypes) {
            emitReadBoxed.invoke(appender, mv, type, 1, 2, 3);
        }

        byte[] localTypes = new byte[] {
                TypeCodes.TYPE_LONG,
                TypeCodes.TYPE_INT,
                TypeCodes.TYPE_STRING,
                TypeCodes.TYPE_BOOLEAN,
                TypeCodes.TYPE_BYTE,
                TypeCodes.TYPE_SHORT,
                TypeCodes.TYPE_CHAR,
                TypeCodes.TYPE_FLOAT,
                TypeCodes.TYPE_DOUBLE
        };
        for (byte type : localTypes) {
            emitStorageToLocal.invoke(appender, mv, type, "io/memris/storage/GeneratedTable", 1, 3, int.class,
                    5, 6, 8, 10, 11);
            emitStorageToLocal.invoke(appender, mv, type, "io/memris/storage/GeneratedTable", 1, 3, Integer.class,
                    5, 6, 8, 10, 11);
            emitLoadForField.invoke(appender, mv, type, int.class, 5, 6, 8, 10, 11);
            emitLoadForField.invoke(appender, mv, type, Integer.class, 5, 6, 8, 10, 11);
        }

        pushInt.invoke(appender, mv, -1);
        pushInt.invoke(appender, mv, 5);
        pushInt.invoke(appender, mv, 100);
        pushInt.invoke(appender, mv, 200);
        pushInt.invoke(appender, mv, 40_000);
    }

}
