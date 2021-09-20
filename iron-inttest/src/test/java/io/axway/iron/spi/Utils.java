package io.axway.iron.spi;

import java.io.*;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import io.axway.alf.log.Logger;
import io.axway.alf.log.LoggerFactory;

import static io.axway.alf.assertion.Assertion.checkState;
import static java.lang.Thread.currentThread;
import static java.util.Comparator.*;

/**
 * This class contains utility methods for the Kafka SPI tests
 */
public final class Utils {
    private static final AtomicInteger PORT = new AtomicInteger(1024);

    /**
     * Tries to recursively delete a folder, silently give up if it cannot be deleted.
     *
     * @param path path to delete
     */
    @SuppressWarnings("ResultOfMethodCallIgnored")
    public static void tryDeleteDirectory(Path path) {
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
    public static void sleep(TimeUnit timeUnit, long duration) {
        try {
            timeUnit.sleep(duration);
        } catch (InterruptedException e) {
            currentThread().interrupt();
            throw new IllegalStateException("Thread was stopped while sleeping", e);
        }
    }

    /**
     * Invokes all the given tasks in the given executor and returns their result
     *
     * @param executorService executor service to use
     * @param tasks           tasks to invoke
     * @param <T>             tasks result type
     * @return the tasks' results
     */
    public static <T> List<T> invokeAll(ExecutorService executorService, List<Callable<T>> tasks) throws Exception {
        List<T> results = new ArrayList<>(tasks.size());
        for (Future<T> future : executorService.invokeAll(tasks)) {
            results.add(future.get());
        }
        return results;
    }

    /**
     * Creates a temporary directory, wraps Java's one without throwing a checked {@link IOException}.
     *
     * @param prefix temporary directory prefix
     * @return path to newly created temporary directory
     */
    public static Path createTempDirectory(String prefix) {
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
    public static Path createDirectories(Path directory) {
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
