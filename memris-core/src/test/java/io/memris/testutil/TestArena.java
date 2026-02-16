package io.memris.testutil;

import io.memris.repository.MemrisArena;
import io.memris.core.MemrisConfiguration;
import io.memris.repository.MemrisRepositoryFactory;

import java.util.function.Consumer;
import java.util.function.Function;

public final class TestArena {

    private TestArena() {
    }

    private static void handleInterruptedException(Exception exception) {
        if (exception instanceof InterruptedException || exception.getCause() instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    public static <T> T withArena(Function<MemrisArena, T> callback) {
        try (var factory = new MemrisRepositoryFactory();
             var arena = factory.createArena()) {
            return callback.apply(arena);
        } catch (Exception exception) {
            handleInterruptedException(exception);
            throw new RuntimeException(exception);
        }
    }

    public static void withArena(Consumer<MemrisArena> callback) {
        try (var factory = new MemrisRepositoryFactory();
             var arena = factory.createArena()) {
            callback.accept(arena);
        } catch (Exception exception) {
            handleInterruptedException(exception);
            throw new RuntimeException(exception);
        }
    }

    public static <T> T withArena(MemrisConfiguration configuration, Function<MemrisArena, T> callback) {
        try (var factory = new MemrisRepositoryFactory(configuration);
             var arena = factory.createArena()) {
            return callback.apply(arena);
        } catch (Exception exception) {
            handleInterruptedException(exception);
            throw new RuntimeException(exception);
        }
    }

    public static void withArena(MemrisConfiguration configuration, Consumer<MemrisArena> callback) {
        try (var factory = new MemrisRepositoryFactory(configuration);
             var arena = factory.createArena()) {
            callback.accept(arena);
        } catch (Exception exception) {
            handleInterruptedException(exception);
            throw new RuntimeException(exception);
        }
    }
}
