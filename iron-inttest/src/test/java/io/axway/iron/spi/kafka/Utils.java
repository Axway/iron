package io.axway.iron.spi.kafka;

import java.io.*;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import com.google.common.util.concurrent.UncheckedExecutionException;

import static io.axway.alf.assertion.Assertion.checkState;
import static java.lang.Thread.currentThread;
import static java.util.Comparator.*;
import static java.util.stream.Collectors.*;

/**
 * This class contains utility methods for the Kafka SPI tests
 */
final class Utils {
    private static final AtomicInteger PORT = new AtomicInteger(1024);

    /**
     * Provides an available port
     *
     * @return available port
     */
    static int providePort() {
        while (true) {
            // Pick a port
            int port = PORT.incrementAndGet();
            checkState(port <= 0xFFFF, "No more ports left to use in test");

            // Check if the port is not already used by another process
            try (ServerSocket ss = new ServerSocket(port); DatagramSocket ds = new DatagramSocket(port)) {
                ss.setReuseAddress(true);
                ds.setReuseAddress(true);
                return port;
            } catch (IOException ignored) {
                // continue, try again
            }
        }
    }

    /**
     * Tries to recursively delete a folder, silently give up if it cannot be deleted.
     *
     * @param path path to delete
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    static void tryDeleteDirectory(Path path) {
        try {
            Files.walk(path).sorted(reverseOrder()).map(Path::toFile).forEach(File::delete);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot delete directory " + path, e);
        }
    }

    /**
     * Sleeps for a given amount of time.
     *
     * @param timeUnit unit of the sleep time
     * @param duration duration of the sleep time
     */
    static void sleep(TimeUnit timeUnit, long duration) {
        try {
            timeUnit.sleep(duration);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new IllegalStateException("Thread was stopped while sleeping", e);
        }
    }

    /**
     * Safely gets the value of a {@link Future} instance.
     *
     * @param future instance
     * @param <T> instance type
     * @return value of the {@link Future} instance
     */
    static <T> T futureGet(Future<T> future) {
        try {
            return future.get();
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new IllegalStateException("Thread was stopped while waiting for the future", e);
        } catch (ExecutionException e) {
            throw new UncheckedExecutionException(e);
        }
    }

    /**
     * Invokes all the given tasks in the given executor and returns their result
     *
     * @param executorService executor service to use
     * @param tasks tasks to invoke
     * @param <T> tasks result type
     * @return the tasks' results
     */
    static <T> List<T> invokeAll(ExecutorService executorService, List<Callable<T>> tasks) {
        try {
            return executorService.invokeAll(tasks).stream().map(Utils::futureGet).collect(toList());
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new IllegalStateException("Thread was stopped while invoking the tasks", e);
        }
    }

    /**
     * Creates a temporary directory, wraps Java's one without throwing a checked {@link IOException}.
     *
     * @param prefix temporary directory prefix
     * @return path to newly created temporary directory
     */
    static Path createTempDirectory(String prefix) {
        try {
            return Files.createTempDirectory(prefix);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create a temporary directory", e);
        }
    }

    /**
     * Creates the directories to the given path, wraps Java's one without throwing a checked {@link IOException}.
     *
     * @param directory directory to create
     * @return directory created
     */
    static Path createDirectories(Path directory) {
        try {
            return Files.createDirectories(directory);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot create the directory " + directory, e);
        }
    }

    private Utils() {
        // Prevent instantiation
    }
}
