package io.axway.iron.core.spi.file;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.*;
import javax.annotation.*;

import static java.util.regex.Pattern.*;

final class FilenameUtils {

    static String buildIdRegex(@Nullable Integer transactionIdLength) {
        String cardinality = transactionIdLength == null ? "+" : ("{" + transactionIdLength + "}");
        return "[0-9]" + cardinality;
    }

    static String buildIdFormat(@Nullable Integer transactionIdLength) {
        return transactionIdLength == null ? "%d" : ("%0" + transactionIdLength + "d");
    }

    static Path ensureDirectoryExists(Path dir) {
        try {
            return Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FilenameUtils() {
        // utility class
    }
}
